package com.worekleszczy.zookeeper.model
import java.nio.file.{Paths, Path => JPAth}
import scala.util._
import scala.util.chaining._
import scala.util.control.NoStackTrace

sealed trait Path extends Any {
  def parent: Path

  def sequential: Option[Long]
  def name: String
  final def raw: String = value.toString

  final def level: Int = {
    if (raw == "/") 0 else raw.count(_ == '/')
  }

  def rebase(newRoot: Path): Path

  def stripBase(base: Path): Path

  /**
    * Transforms current file name. If path had associated any sequential number it's lost
    * @param func function transforming current file name
    * @return
    */
  def transformFileName(func: String => String): Path

  def extractSequential(func: String => Option[SequentialContext]): Path

//  def extractSequentialWithSeparator(separator: Char): Path

  def resolve(value: String): Path

  private[model] def value: JPAth
}

case class SequentialContext(nameOnly: String, sequential: Long)

private[model] final case class PathImpl(
  private[model] val value: JPAth,
  private[model] val sequentialContext: Option[SequentialContext]
) extends Path {

  def name: String = sequentialContext.map(_.nameOnly).getOrElse(value.getFileName.toString)

  def parent: Path = PathImpl(value.getParent.normalize(), None)

  def sequential: Option[Long] = sequentialContext.map(_.sequential)

  def rebase(root: Path): Path =
    copy(value = root.value.resolve(value.toString.substring(1)).normalize())

  def stripBase(base: Path): Path = {

    if (base.raw == "/") {
      this
    } else copy(value = Paths.get(raw.stripPrefix(base.raw)))

  }

  def extractSequential(func: String => Option[SequentialContext]): Path = copy(sequentialContext = func(name))

  def resolve(child: String): Path = {
    (if (child.startsWith("/")) child.substring(1) else child).pipe(x => PathImpl(value.resolve(x).normalize(), None))

  }

  def transformFileName(func: String => String): Path =
    PathImpl(value.getParent.resolve(func(value.getFileName.toString)), None)

  override lazy val toString = s"Path(${value.toString}, $sequential)"
}

object Path {

  val root: Path = Path.unsafeFromString("/")

  sealed abstract class PathException extends RuntimeException with NoStackTrace

  final case class InvalidPathException(path: String) extends PathException {
    override lazy val getMessage: String = s"Path $path is invalid"
  }

  final case class InvalidSequentialNumber(number: String) extends PathException {
    override lazy val getMessage: String = s"Sequential $number is invalid"
  }

  final case class PathPrefixException(path: String, name: String) extends PathException {
    override lazy val getMessage: String = s"$name doesn't starts with a $path"
  }

  def apply(raw: String): Try[Path] =
    Try(Paths.get(raw)).flatMap { path =>
      if (path.isAbsolute) {
        Success(PathImpl(path.normalize(), None))
      } else Failure(InvalidPathException(raw))
    }

  def apply(raw: String, sequentialContext: SequentialContext): Try[Path] =
    Try(Paths.get(raw)).flatMap { path =>
      if (path.isAbsolute) {
        Success(PathImpl(path.normalize(), Some(sequentialContext)))
      } else Failure(InvalidPathException(raw))
    }

  def unsafeFromString(raw: String): Path = apply(raw).get

  def asAbsolutePath(raw: String): Try[Path] = {
    val absolute = if (raw.startsWith("/")) raw else s"/$raw"
    apply(absolute)
  }

  def unsafeAsAbsolutePath(raw: String): Path = asAbsolutePath(raw).get

  implicit val ordering: Ordering[Path] = Ordering.by(path => (path.sequential.getOrElse(Long.MaxValue), path.raw))

}

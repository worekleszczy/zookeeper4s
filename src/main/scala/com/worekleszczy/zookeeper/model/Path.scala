package com.worekleszczy.zookeeper.model
import java.nio.file.{Paths, Path => JPAth}
import scala.util._
import scala.util.control.NoStackTrace

sealed trait Path extends Any {
  def parent: Path

  def sequential: Option[Long]
  final def name: String = value.getFileName.toString
  final def raw: String  = value.toString

  final def level: Int = {
    if (raw == "/") 0 else raw.count(_ == '/')
  }

  def rebase(newRoot: Path): Path

  def stripBase(base: Path): Path

  private[model] def value: JPAth
}

private[model] final case class PathImpl(private[model] val value: JPAth, val sequential: Option[Long]) extends Path {
  def parent: Path = PathImpl(value.getParent.normalize(), None)

  def rebase(root: Path): Path =
    PathImpl(root.value.resolve(value.toString.substring(1)).normalize(), sequential)

  def stripBase(base: Path): Path = {

    if (base.raw == "/") {
      this
    } else PathImpl(Paths.get(raw.stripPrefix(base.raw)), sequential)

  }

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

  def apply(raw: String, sequential: Long): Try[Path] =
    Try(Paths.get(raw)).flatMap { path =>
      if (path.isAbsolute) {
        Success(PathImpl(path.normalize(), Some(sequential)))
      } else Failure(InvalidPathException(raw))
    }

  def unsafeFromString(raw: String): Path = apply(raw).get

  def asAbsolutePath(raw: String): Try[Path] = {
    val absolute = if (raw.startsWith("/")) raw else s"/$raw"
    apply(absolute)
  }

  def unsafeAsAbsolutePath(raw: String): Path = asAbsolutePath(raw).get

  /*
    Path: /bacchus/another_bites
    Name: /bacchus/another_bites0000000027
   */
  def fromPathAndName(path: String, name: String): Try[Path] = {

    if (name.startsWith(path)) {
      val sequential = name.substring(path.length)

      if (sequential.nonEmpty) {
        sequential.toLongOption.fold[Try[Path]](Failure(InvalidSequentialNumber(sequential)))(apply(name, _))
      } else apply(path)
    } else Failure(PathPrefixException(path, name))

  }

  def unsafeFromPathAndName(path: String, name: String): Path = fromPathAndName(path, name).get
}

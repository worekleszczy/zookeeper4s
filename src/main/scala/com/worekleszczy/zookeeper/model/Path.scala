package com.worekleszczy.zookeeper.model
import java.nio.file.{Paths, Path => JPAth}
import scala.util._
import scala.util.control.NoStackTrace

sealed trait Path extends Any {
  def parent: Path
  def name: String
  def raw: String

  def rebase(newRoot: Path): Path

  private[model] def value: JPAth
}

private[model] final class PathImpl(private[model] val value: JPAth) extends Path {
  def parent: Path = new PathImpl(value.getParent.normalize())

  def name: String = value.getFileName.toString

  def raw: String = value.toString

  def rebase(r: Path): Path =
    r match {
      case newRoot: PathImpl =>
        new PathImpl(newRoot.value.resolve(value.toString.substring(1)).normalize())
    }

}

object Path {

  final case class InvalidPathException(path: String) extends RuntimeException with NoStackTrace {
    override lazy val getMessage: String = s"Path $path is invalid"
  }

  def apply(raw: String): Try[Path] =
    Try(Paths.get(raw)).flatMap { path =>
      if (path.isAbsolute) {
        Success(new PathImpl(path.normalize()))
      } else Failure(new InvalidPathException(raw))
    }

  def unsafeFromString(raw: String): Path = apply(raw).get
}

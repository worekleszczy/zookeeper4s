package com.worekleszczy.zookeeper.model
import java.nio.file.Files
import java.nio.file.{Paths, Path => JPAth}
import scala.util.Try

sealed trait Path extends Any {
  def parent: Path
  def name: String
  def absolutePathRaw: String
}

private final class PathImpl(private val value: JPAth) extends Path {
  def parent: Path = new PathImpl(value.getParent)

  def name: String = value.getFileName.toString

  def absolutePathRaw: String = value.toString
}

object Path {

  def apply(raw: String): Option[Path] = {
    Try {
      val path = Paths.get(raw).toAbsolutePath
      new PathImpl(path)
    }.toOption
  }

  def unsafeFromString(raw: String): Path = apply(raw).getOrElse(throw new RuntimeException(s"Illegal path $raw"))
}

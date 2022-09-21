package com.worekleszczy.zookeeper.config
import cats.Functor
import cats.effect.std.UUIDGen
import cats.syntax.functor._
import com.softwaremill.tagging._
import com.worekleszczy.zookeeper.model.Path

import scala.concurrent.duration._

sealed trait InstanceId

case class ZookeeperConfig private (
  root: Path,
  host: String,
  port: Int,
  timeout: FiniteDuration,
  id: String @@ InstanceId,
  createRootIfNotExists: Boolean,
  removeRootOnExit: Boolean
)

object ZookeeperConfig {

  def create[F[_]: UUIDGen: Functor](
    root: Path,
    host: String,
    port: Int,
    timeout: FiniteDuration,
    createRootIfNotExists: Boolean = true,
    removeRootOnExit: Boolean = false
  ): F[ZookeeperConfig] =
    UUIDGen[F].randomUUID.map(id =>
      ZookeeperConfig(
        root,
        host,
        port,
        timeout,
        id.toString.taggedWith[InstanceId],
        createRootIfNotExists,
        removeRootOnExit
      )
    )

  def default[F[_]: UUIDGen: Functor](root: Path): F[ZookeeperConfig] = create(root, "localhost", 2181, 3.seconds)
}

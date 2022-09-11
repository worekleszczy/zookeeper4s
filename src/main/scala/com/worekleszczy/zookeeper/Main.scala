package com.worekleszczy.zookeeper

import cats.effect.kernel.Deferred
import cats.effect.std.Dispatcher
import cats.effect.syntax.all._
import cats.effect.{IO, IOApp}
import com.worekleszczy.zookeeper.Zookeeper.noopWatcher
import com.worekleszczy.zookeeper.config.ZookeeperConfig
import com.worekleszczy.zookeeper.model.Path
import org.apache.zookeeper.WatchedEvent
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

object Main extends IOApp.Simple {

  implicit val logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  def run: IO[Unit] = {

    val setUpApp = for {
      root       <- IO.fromTry(Path("/bacchus")).toResource
      config     <- ZookeeperConfig.default[IO](root).toResource
      dispatcher <- Dispatcher[IO]
      zookeeper  <- Zookeeper[IO](config, dispatcher, noopWatcher)
    } yield (root, config, dispatcher, zookeeper)

    setUpApp.use {
      case (_, _, _, zookeeper) =>
        for {
          listDir <- IO.fromTry(Path("/"))
          _ <-
            EventStream
              .watchChildren(zookeeper, listDir)
              .evalTap(children => IO.println(children.map(_.name).mkString(",")))
              .compile
              .drain
          _ <- IO.never[Unit]
        } yield ()
    }

  }

  def completableWatcher(deferred: Deferred[IO, WatchedEvent]): Watcher[IO] =
    event => {
      deferred.complete(event).void
    }

}

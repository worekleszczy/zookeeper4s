package com.worekleszczy.zookeeper

import cats.effect.std.{Dispatcher, Queue}
import cats.effect.{Deferred, IO, IOApp, Resource}
import fs2._
import org.apache.zookeeper.AsyncCallback.Children2Callback
import org.apache.zookeeper.{WatchedEvent, Watcher, ZooKeeper}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.log4cats.syntax._

import scala.concurrent.duration.{FiniteDuration, _}
import scala.jdk.CollectionConverters._

case class ZookeeperConfig(host: String, timeout: FiniteDuration, applicationNode: String)

object Main extends IOApp.Simple {

  implicit val logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  // This is your new "main"!
  def run: IO[Unit] = {

    val resource = for {
      c       <- config
      d       <- Dispatcher[IO]
      watcher <- Resource.pure[IO, Watcher](_ => ())
      z       <- zookeeper(c, watcher)
    } yield (z, d)

    resource.use {
      case (zookeeper, dispatcher) =>
        def emitChildren(watcher: Watcher) =
          IO.async_[List[String]] { callback =>
            val cb: Children2Callback = (_, _, _, children, _) => {
              callback(Right(children.asScala.toList))
            }

            zookeeper.getChildren("/bacchus", watcher, cb, ())
          }

        def completableWatcher(deferred: Deferred[IO, WatchedEvent]): Watcher =
          event => {
            dispatcher.unsafeRunAndForget(deferred.complete(event))
          }

        val childrenStream = for {
          signal <- Stream.eval(Deferred[IO, WatchedEvent])
          watcher = completableWatcher(signal)
          children <- (Stream.eval(emitChildren(watcher)) ++ Stream.eval(signal.get).drain)
        } yield children

        val appStream = childrenStream.repeat
          .evalTap(currentNodes => info"Current Nodes $currentNodes")
          .drain

        for {
          _ <- appStream.compile.drain
          _ <- IO.never[Unit]
        } yield ()

    }
  }

  def config: Resource[IO, ZookeeperConfig] = Resource.pure(ZookeeperConfig("localhost:2181", 3.seconds, "/bacchus"))

  def queue: Resource[IO, Queue[IO, WatchedEvent]] = Resource.eval(Queue.unbounded[IO, WatchedEvent])

  def enqueueWatcher(queue: Queue[IO, WatchedEvent], dispatcher: Dispatcher[IO]): Resource[IO, Watcher] = {
    val watcher = new Watcher {
      def process(event: WatchedEvent): Unit = {

        dispatcher.unsafeRunAndForget(queue.offer(event))
      }
    }

    Resource.pure(watcher)
  }

  def zookeeper(config: ZookeeperConfig, watcher: Watcher): Resource[IO, ZooKeeper] =
    Resource.make {
      IO {
        new ZooKeeper(config.host, config.timeout.toMillis.toInt, watcher)
      }
    } { z => IO.delay(z.close()) }

}

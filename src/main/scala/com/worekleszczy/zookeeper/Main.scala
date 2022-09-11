package com.worekleszczy.zookeeper

import cats.effect.kernel.Deferred
import cats.effect.std.Dispatcher
import cats.effect.{IO, IOApp}
import cats.syntax.foldable._
import com.worekleszczy.zookeeper.config.ZookeeperConfig
import com.worekleszczy.zookeeper.model.Path
import org.apache.zookeeper.WatchedEvent
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import cats.effect.syntax.all._
import com.worekleszczy.zookeeper.Zookeeper.noopWatcher

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
          listDir  <- IO.fromTry(Path("/"))
          deferred <- Deferred[IO, WatchedEvent]
          watcher = completableWatcher(deferred)
          children1 <- zookeeper.getChildren(listDir, watcher)
          _         <- children1.map(child => IO.println(child.name)).sequence_
          _         <- IO.println("Waiting for a change")
          _         <- deferred.get
          children2 <- zookeeper.getChildren(listDir, watch = false)
          _         <- children2.map(child => IO.println(child.name)).sequence_
        } yield ()
    }

  }

  def completableWatcher(deferred: Deferred[IO, WatchedEvent]): Watcher[IO] =
    event => {
      deferred.complete(event).void
    }
//    event => {
//      dispatcher.unsafeRunAndForget(deferred.complete(event))
//    }

  //  // This is your new "main"!
//  def run: IO[Unit] = {
//
//    val resource = for {
//      c       <- config
//      d       <- Dispatcher[IO]
//      watcher <- Resource.pure[IO, Watcher](_ => ())
//      z       <- zookeeper(c, watcher)
//    } yield (z, d)
//
//    resource.use {
//      case (zookeeper, dispatcher) =>
//        def emitChildren(watcher: Watcher) =
//          IO.async_[List[String]] { callback =>
//            val cb: Children2Callback = (_, _, _, children, _) => {
//              callback(Right(children.asScala.toList))
//            }
//
//            zookeeper.getChildren("/bacchus", watcher, cb, ())
//          }
//
//        def completableWatcher(deferred: Deferred[IO, WatchedEvent]): Watcher =
//          event => {
//            dispatcher.unsafeRunAndForget(deferred.complete(event))
//          }
//
//        val childrenStream = for {
//          signal <- Stream.eval(Deferred[IO, WatchedEvent])
//          watcher = completableWatcher(signal)
//          children <- (Stream.eval(emitChildren(watcher)) ++ Stream.eval(signal.get).drain)
//        } yield children
//
//        val appStream = childrenStream.repeat
//          .evalTap(currentNodes => info"Current Nodes $currentNodes")
//          .drain
//
//        for {
//          _ <- appStream.compile.drain
//          _ <- IO.never[Unit]
//        } yield ()
//
//    }
//  }
//
//  def config: Resource[IO, ZookeeperConfig] = ???
//
//  def queue: Resource[IO, Queue[IO, WatchedEvent]] = Resource.eval(Queue.unbounded[IO, WatchedEvent])
//
//  def enqueueWatcher(queue: Queue[IO, WatchedEvent], dispatcher: Dispatcher[IO]): Resource[IO, Watcher] = {
//    val watcher = new Watcher {
//      def process(event: WatchedEvent): Unit = {
//
//        dispatcher.unsafeRunAndForget(queue.offer(event))
//      }
//    }
//
//    Resource.pure(watcher)
//  }
//
//  def zookeeper(config: ZookeeperConfig, watcher: Watcher): Resource[IO, ZooKeeper] =
//    Resource.make {
//      IO {
//        new ZooKeeper(config.host, config.timeout.toMillis.toInt, watcher)
//      }
//    } { z => IO.delay(z.close()) }

}

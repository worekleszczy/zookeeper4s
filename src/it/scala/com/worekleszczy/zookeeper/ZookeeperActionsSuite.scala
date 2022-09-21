package com.worekleszczy.zookeeper

import cats.effect.std.{Dispatcher, Queue, UUIDGen}
import cats.effect.syntax.all._
import cats.effect.{Deferred, IO, Resource}
import cats.syntax.applicative._
import cats.syntax.monadError._
import cats.syntax.option._
import cats.syntax.traverse._
import com.worekleszczy.zookeeper.Zookeeper.{noopWatcher, ZookeeperClientError, ZookeeperLive}
import com.worekleszczy.zookeeper.codec.ByteCodec.syntax._
import com.worekleszczy.zookeeper.config.ZookeeperConfig
import com.worekleszczy.zookeeper.model.Path
import fs2.Stream
import munit.CatsEffectSuite
import org.apache.zookeeper.Watcher.Event.EventType
import org.apache.zookeeper.{CreateMode, KeeperException, WatchedEvent}
import org.testcontainers.containers.GenericContainer
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.log4cats.syntax._

import scala.concurrent.duration._
import scala.util.Try

class ZookeeperActionsSuite extends CatsEffectSuite {

  final class ZooKeeperContainer extends GenericContainer[ZooKeeperContainer]("zookeeper:3.7.1")

  implicit val logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  private val startZookeeperResource = info"Start zookeeper" >> IO {
    val container = new ZooKeeperContainer().withExposedPorts(2181)
    container.start()
    container
  }.flatTap { container =>
    IO.fromTry(Try(container.getMappedPort(2181))).flatMap(port => info"Container exposed port: $port")
  }

  def stopZookeeper(container: ZooKeeperContainer): IO[Unit] =
    info"Stopping zookeeper" *> IO {
      container.stop()
    }

  private val zookeeperContainer =
    ResourceSuiteLocalFixture("zookeeper", Resource.make(startZookeeperResource)(stopZookeeper))

  def zookeeper(
    watcher: Watcher[IO] = noopWatcher[IO],
    container: ZooKeeperContainer = zookeeperContainer()
  ): Resource[IO, (ZookeeperLive[IO], String)] =
    for {
      exposed <- (IO.fromTry(Try(container.getMappedPort(2181)))).toResource
      id      <- UUIDGen[IO].randomUUID.toResource
      config <-
        ZookeeperConfig
          .create[IO](Path.unsafeAsAbsolutePath(id.toString), "localhost", exposed, 10.seconds, true, true)
          .toResource
      dispatcher <- Dispatcher[IO]
      zookeeper  <- Zookeeper.createLiveZookeeper[IO](config, dispatcher, watcher)
    } yield (zookeeper, id.toString)

  override lazy val munitFixtures = List(zookeeperContainer)

  test("create node and list nodes") {

    zookeeper().use {
      case (zookeeper, _) =>
        for {
          _ <- zookeeper.createEmpty(Path.unsafeFromString("/testnode"), CreateMode.PERSISTENT)
          _ <- assertIO(
            zookeeper.getChildren(Path.unsafeFromString("/"), false).rethrow,
            Vector(Path.unsafeFromString("/testnode"))
          )
        } yield ()
    }
  }

  test("write a body to a node and read it back unchanged") {
    zookeeper().use {
      case (zookeeper, _) =>
        val testNodeExpectedValue = "alamakotaakotmaale"
        for {
          encoded <- IO.fromTry(testNodeExpectedValue.encode)
          testNodePath = Path.unsafeFromString("/testnode")
          _                  <- zookeeper.create(testNodePath, encoded, CreateMode.PERSISTENT)
          (testNodeValue, _) <- zookeeper.getData[String](Path.unsafeFromString("/testnode"), false).rethrow
          _                  <- assertIO(testNodeValue.pure[IO], testNodeExpectedValue)
        } yield ()
    }
  }

  test("delete node when the version match") {
    zookeeper().use {
      case (zookeeper, _) =>
        val testNodePath = Path.unsafeFromString("/testnode")
        for {
          (_, stats) <- zookeeper.createEmpty(testNodePath, CreateMode.PERSISTENT).rethrow
          _          <- zookeeper.delete(testNodePath, stats.getVersion).rethrow
          _          <- assertIO(zookeeper.getChildren(Path.root, false).rethrow, Vector.empty)
        } yield ()
    }
  }

  test("not allow removing a node with wrong version") {
    zookeeper().use {
      case (zookeeper, _) =>
        val testNodePath = Path.unsafeFromString("/testnode")
        for {
          (_, stats) <- zookeeper.createEmpty(testNodePath, CreateMode.PERSISTENT).rethrow
          _          <- zookeeper.setData(testNodePath, Array.empty, stats.getVersion).rethrow
          _ <- assertIO(
            zookeeper.delete(testNodePath, stats.getVersion),
            Left(ZookeeperClientError(KeeperException.Code.BADVERSION))
          )
        } yield ()
    }
  }

  test("get notified only after a file is created change happened") {
    zookeeper().use {
      case (zookeeper, id) =>
        val testNodePath = Path.unsafeFromString("/testnode")
        for {
          deferred <- Deferred[IO, WatchedEvent]
          watcher = Watcher.instance(deferred.complete)
          _     <- assertIO(zookeeper.exists(testNodePath, watcher).rethrow, none)
          _     <- assertIO(deferred.tryGet, none)
          _     <- zookeeper.createEmpty(testNodePath, CreateMode.PERSISTENT).rethrow
          event <- deferred.get
          _     <- assertIO(event.getPath.pure[IO], s"/$id/testnode")
          _     <- assertIO(event.getType.pure[IO], EventType.NodeCreated)
        } yield ()
    }
  }

  test("notify global watcher after a change happened") {

    val setUp = for {
      deferred <- Queue.unbounded[IO, WatchedEvent].toResource
      watcher = Watcher.instance(deferred.offer)
      (zookeeper, id) <- zookeeper(watcher)
    } yield (zookeeper, id, deferred)

    setUp.use {
      case (zookeeper, id, events) =>
        val testNodePath = Path.unsafeFromString("/testnode")
        for {
          _ <- assertIO(zookeeper.exists(testNodePath, watch = true).rethrow, none)
          allAvailable =
            Stream
              .repeatEval(events.tryTake)
              .collectWhile {
                case Some(x) => x
              }
              .compile
              .toVector
          _ <- assertIOBoolean(allAvailable.map(_.forall(_.getType != EventType.NodeCreated)))
          _ <- zookeeper.createEmpty(testNodePath, CreateMode.PERSISTENT).rethrow
          event <- allAvailable.flatMap { events =>
            val createdEvents = events.filter(_.getType == EventType.NodeCreated)

            if (createdEvents.size > 1) {
              IO(fail("Multiple NodeCreated events found"))
            } else createdEvents.headOption.fold[IO[WatchedEvent]](IO(fail("No NodeCreated event found")))(IO.pure)
          }
          _ <- assertIO(event.getPath.pure[IO], s"/$id/testnode")
          _ <- assertIO(event.getType.pure[IO], EventType.NodeCreated)
        } yield ()
    }
  }

//  test("the one about serial when listing") {
//    zookeeper().use {
//      case (zookeeper, id) =>
//        val testNodePath1 = Path.unsafeFromString("/testnode1")
//        val testNodePath2 = Path.unsafeFromString("/testnode2")
//        val testNodePath3 = Path.unsafeFromString("/testnode3")
//
//        val paths = Vector(testNodePath1, testNodePath2, testNodePath3)
//
//        for {
//
//          _     <- paths.map(zookeeper.createEmpty(_, CreateMode.PERSISTENT_SEQUENTIAL).rethrow).sequence
//          nodes <- zookeeper.getChildren(Path.root, false).rethrow
//          _     <- assertIO(nodes.flatMap(_.sequential).size.pure[IO], 3)
////          _ <- assertIO(nodes.sortBy(_.sequential.getOrElse(Long.MaxValue)).map(_.name))
//        } yield ()
//    }
//  }

  test("get notified only after a file is deleted change happened") {
    zookeeper().use {
      case (zookeeper, id) =>
        val testNodePath = Path.unsafeFromString("/testnode")
        for {
          deferred <- Deferred[IO, WatchedEvent]
          watcher = Watcher.instance(deferred.complete)
          _         <- zookeeper.createEmpty(testNodePath, CreateMode.PERSISTENT).rethrow
          (_, stat) <- zookeeper.getData[String](testNodePath, watcher).rethrow
          _         <- assertIO(deferred.tryGet, none)
          _         <- zookeeper.delete(testNodePath, stat.getVersion)
          event     <- deferred.get
          _         <- assertIO(event.getPath.pure[IO], s"/$id/testnode")
          _         <- assertIO(event.getType.pure[IO], EventType.NodeDeleted)
        } yield ()
    }
  }

}

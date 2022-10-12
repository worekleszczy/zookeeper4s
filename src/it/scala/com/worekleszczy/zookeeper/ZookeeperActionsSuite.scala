package com.worekleszczy.zookeeper

import cats.effect.std.{Dispatcher, Queue, UUIDGen}
import cats.effect.syntax.all._
import cats.effect.{Deferred, IO, Resource}
import cats.syntax.applicative._
import cats.syntax.monadError._
import cats.syntax.option._
import cats.syntax.traverse._
import com.worekleszczy.zookeeper.Zookeeper.syntax._
import com.worekleszczy.zookeeper.Zookeeper.{ZookeeperClientError, ZookeeperLive, noopWatcher}
import com.worekleszczy.zookeeper.codec.ByteCodec.syntax._
import com.worekleszczy.zookeeper.config.ZookeeperConfig
import com.worekleszczy.zookeeper.model.Path
import fs2.Stream
import munit.CatsEffectSuite
import org.apache.zookeeper.Watcher.Event.EventType
import org.apache.zookeeper.{AddWatchMode, CreateMode, KeeperException, WatchedEvent}
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

  test("should return none when getData is executed for non existing node") {

    zookeeper().use {
      case (zookeeper, _) =>
        for {
          result <- zookeeper.getData[String](Path.unsafeFromString("/testnode"), false).rethrow
          _      <- assertIO(result.pure[IO], none)
        } yield ()
    }
  }

  test("should return none when getData is executed for non existing node") {

    zookeeper().use {
      case (zookeeper, _) =>
        for {
          result <- zookeeper.getData[String](Path.unsafeFromString("/testnode"), false).rethrow
          _      <- assertIO(result.pure[IO], none)
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
          (testNodeValue, _) <- zookeeper.unsafeGetData[String](Path.unsafeFromString("/testnode"), false).rethrow
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
          watcher = Watcher.instance(deferred.complete(_).void)
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

  test("the one about serial when listing") {
    zookeeper().use {
      case (zookeeper, id) =>
        val testNodePath1 = Path.unsafeFromString("/testnode1")
        val testNodePath2 = Path.unsafeFromString("/testnode2")
        val testNodePath3 = Path.unsafeFromString("/testnode3")

        val paths = Vector(testNodePath1, testNodePath2, testNodePath3)

        for {

          _     <- paths.map(zookeeper.createEmpty(_, CreateMode.PERSISTENT_SEQUENTIAL).rethrow).sequence
          nodes <- zookeeper.getChildren(Path.root, false).rethrow
          _     <- assertIO(nodes.flatMap(_.sequential).size.pure[IO], 3)
          _ <- assertIO(
            nodes.filter(_.sequential.isDefined).sortBy(_.sequential.get).map(_.name).pure[IO],
            Vector("testnode1", "testnode2", "testnode3")
          )
        } yield ()
    }
  }

  test("get notified only after a file is deleted change happened") {
    zookeeper().use {
      case (zookeeper, id) =>
        val testNodePath = Path.unsafeFromString("/testnode")
        for {
          deferred <- Deferred[IO, WatchedEvent]
          watcher = Watcher.instance(deferred.complete(_).void)
          _         <- zookeeper.createEmpty(testNodePath, CreateMode.PERSISTENT).rethrow
          (_, stat) <- zookeeper.unsafeGetData[String](testNodePath, watcher).rethrow
          _         <- assertIO(deferred.tryGet, none)
          _         <- zookeeper.delete(testNodePath, stat.getVersion)
          event     <- deferred.get
          _         <- assertIO(event.getPath.pure[IO], s"/$id/testnode")
          _         <- assertIO(event.getType.pure[IO], EventType.NodeDeleted)
        } yield ()
    }
  }

  test("add persistent watcher and remove on close") {
    zookeeper().use {
      case (zookeeper, id) =>
        val testNodePath = Path.unsafeFromString("/test_container")
        val childA       = testNodePath.resolve("a")
        val childB       = testNodePath.resolve("b")
        for {
          eventQueue <- Queue.unbounded[IO, WatchedEvent]
          watcher = Watcher.instance(eventQueue.offer)
          allAvailable =
            Stream
              .repeatEval(eventQueue.tryTake)
              .collectWhile {
                case Some(x) => x
              }
              .compile
              .toVector
          (_, initialStats) <- zookeeper.createEmpty(testNodePath, CreateMode.CONTAINER).rethrow
          lastUpdateStats <- zookeeper.addWatcher(testNodePath, watcher, AddWatchMode.PERSISTENT).use { _ =>
            for {
              firstUpdateStats        <- zookeeper.setDataEncode(testNodePath, "first", initialStats.getVersion).rethrow
              secondUpdateStats       <- zookeeper.setDataEncode(testNodePath, "second", firstUpdateStats.getVersion).rethrow
              (_, childAInitialStats) <- zookeeper.createEmpty(childA, CreateMode.EPHEMERAL).rethrow
              _                       <- zookeeper.setDataEncode(childA, "childANewContent", childAInitialStats.getVersion).rethrow
              generatedEvents         <- allAvailable
              _ <- assertIO(
                generatedEvents.map(_.getType).pure[IO],
                Vector.fill(2)(EventType.NodeDataChanged) :+ EventType.NodeChildrenChanged
              )
            } yield secondUpdateStats
          }
          _                       <- allAvailable // we drop all events that are in queue left after a watcher was removed
          thirdUpdateStats        <- zookeeper.setDataEncode(testNodePath, "third", lastUpdateStats.getVersion).rethrow
          _                       <- zookeeper.setDataEncode(testNodePath, "forth", thirdUpdateStats.getVersion).rethrow
          (_, childBInitialStats) <- zookeeper.createEmpty(childB, CreateMode.EPHEMERAL).rethrow
          _                       <- zookeeper.setDataEncode(childB, "childANewContent", childBInitialStats.getVersion).rethrow

          remainingEvents <- allAvailable
          _ <- assertIO(
            remainingEvents.pure[IO],
            Vector.empty
          )

        } yield ()
    }
  }

  test("add persistent recurisve watcher and remove on close") {
    zookeeper().use {
      case (zookeeper, id) =>
        val testNodePath = Path.unsafeFromString("/test_container")
        val childA       = testNodePath.resolve("a")
        val childB       = testNodePath.resolve("b")
        for {
          eventQueue <- Queue.unbounded[IO, WatchedEvent]
          watcher = Watcher.instance(eventQueue.offer)
          allAvailable =
            Stream
              .repeatEval(eventQueue.tryTake)
              .collectWhile {
                case Some(x) => x
              }
              .compile
              .toVector
          (_, initialStats) <- zookeeper.createEmpty(testNodePath, CreateMode.CONTAINER).rethrow
          lastUpdateStats <- zookeeper.addWatcher(testNodePath, watcher, AddWatchMode.PERSISTENT_RECURSIVE).use { _ =>
            for {
              firstUpdateStats        <- zookeeper.setDataEncode(testNodePath, "first", initialStats.getVersion).rethrow
              secondUpdateStats       <- zookeeper.setDataEncode(testNodePath, "second", firstUpdateStats.getVersion).rethrow
              (_, childAInitialStats) <- zookeeper.createEmpty(childA, CreateMode.EPHEMERAL).rethrow
              _                       <- zookeeper.setDataEncode(childA, "childANewContent", childAInitialStats.getVersion).rethrow
              generatedEvents         <- allAvailable
              _ <- assertIO(
                generatedEvents.map(_.getType).pure[IO],
                Vector.fill(2)(EventType.NodeDataChanged) :+ EventType.NodeCreated :+ EventType.NodeDataChanged
              )
            } yield secondUpdateStats
          }
          _                       <- allAvailable // we drop all events that are in queue left after a watcher was removed
          thirdUpdateStats        <- zookeeper.setDataEncode(testNodePath, "third", lastUpdateStats.getVersion).rethrow
          _                       <- zookeeper.setDataEncode(testNodePath, "forth", thirdUpdateStats.getVersion).rethrow
          (_, childBInitialStats) <- zookeeper.createEmpty(childB, CreateMode.EPHEMERAL).rethrow
          _                       <- zookeeper.setDataEncode(childB, "childANewContent", childBInitialStats.getVersion).rethrow

          remainingEvents <- allAvailable
          _ <- assertIO(
            remainingEvents.pure[IO],
            Vector.empty
          )

        } yield ()
    }
  }

}

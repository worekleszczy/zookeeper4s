package com.worekleszczy.zookeeper

import cats.effect.std.{Dispatcher, UUIDGen}
import cats.effect.syntax.all._
import cats.effect.{IO, Resource}
import com.worekleszczy.zookeeper.Zookeeper.{noopWatcher, ZookeeperLive}
import com.worekleszczy.zookeeper.config.ZookeeperConfig
import com.worekleszczy.zookeeper.model.Path
import com.worekleszczy.zookeeper.syntax._
import munit.CatsEffectSuite
import org.apache.zookeeper.CreateMode
import org.testcontainers.containers.GenericContainer
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.log4cats.syntax._

import scala.concurrent.duration._
import scala.util.Try

class AsyncOpsSuite extends CatsEffectSuite {

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

  test("wait until other client creates a node") {

    zookeeper().both(zookeeper()).use {
      case ((zookeeper, id), (zookeeper2, _)) =>
        val lockNode = Path.unsafeFromString("/testnode")
        val nodeRaw  = "alamakotaakotnielubiali"

        for {
          _ <- (IO.sleep(5.seconds) >> zookeeper2.asAbsolute
              .createEncode(lockNode.rebase(Path.unsafeAsAbsolutePath(id)), nodeRaw, CreateMode.PERSISTENT)).start

          _ <- assertIO(
            zookeeper.unsafeGetDataBlock[String](lockNode),
            nodeRaw
          )
        } yield ()
    }
  }

  test("return right away if node exists") {

    zookeeper().use {
      case (zookeeper, id) =>
        val lockNode = Path.unsafeFromString("/testnode")
        val nodeRaw  = "alamakotaakotnielubiali"

        for {
          _ <-
            zookeeper
              .createEncode(lockNode, nodeRaw, CreateMode.PERSISTENT)
          _ <- assertIO(
            zookeeper.unsafeGetDataBlock[String](lockNode).timeout(1.second),
            nodeRaw
          )
        } yield ()
    }
  }

}

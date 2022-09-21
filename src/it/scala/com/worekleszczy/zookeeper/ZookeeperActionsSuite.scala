package com.worekleszczy.zookeeper

import cats.effect.std.{Dispatcher, UUIDGen}
import cats.effect.syntax.all._
import cats.effect.{IO, Resource}
import cats.syntax.monadError._
import com.worekleszczy.zookeeper.Zookeeper.{noopWatcher, ZookeeperLive}
import com.worekleszczy.zookeeper.config.ZookeeperConfig
import com.worekleszczy.zookeeper.model.Path
import munit.CatsEffectSuite
import org.apache.zookeeper.CreateMode
import org.testcontainers.containers.GenericContainer
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.log4cats.syntax._

import scala.concurrent.duration._
import scala.util.Try

class ZookeeperActionsSuite extends CatsEffectSuite {

  final class ZooKeeperContainer extends GenericContainer[ZooKeeperContainer]("zookeeper:3.7.1")

  implicit val logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  private val startZookeeperResource = debug"Start zookeeper" >> IO {
    val container = new ZooKeeperContainer().withExposedPorts(2181)
    container.start()
    container
  }

  def stopZookeeper(container: ZooKeeperContainer): IO[Unit] =
    debug"Stopping zookeeper" *> IO {
      container.stop()
    }

  private val zookeeperContainer =
    ResourceSuiteLocalFixture("zookeeper", Resource.make(startZookeeperResource)(stopZookeeper))

  def zookeeper(container: ZooKeeperContainer = zookeeperContainer()): Resource[IO, (ZookeeperLive[IO], String)] =
    for {
      exposed <-
        (IO.fromTry(Try(container.getMappedPort(2181))).flatTap(exposed => info"Exposed port: $exposed")).toResource
      id <- UUIDGen[IO].randomUUID.toResource
      config <-
        ZookeeperConfig
          .create[IO](Path.unsafeAsAbsolutePath(id.toString), "localhost", exposed, 10.seconds, true)
          .toResource
      dispatcher <- Dispatcher[IO]
      zookeeper  <- Zookeeper.createLiveZookeeper[IO](config, dispatcher, noopWatcher)
    } yield (zookeeper, id.toString)

  override lazy val munitFixtures = List(zookeeperContainer)

//  test("should create a root key when createRootIfNotExists=true") {
//    val test = zookeeper()
//
//    val startup = for {
//      exposed   <- (IO.fromTry(Try(test.getMappedPort(2181))).flatTap(exposed => info"Exposed port: $exposed")).toResource
//      config    <- ZookeeperConfig.create[IO](Path.unsafeFromString("/test-1"), "localhost", exposed, 10.seconds, true).toResource
//      zookeeper <- Zookeeper.createLiveZookeeper()
//    } yield zookeeper
//
//    startup.use { zookeeper =>
//
//      zookeeper.getChi
//
//    }
//  }

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

}

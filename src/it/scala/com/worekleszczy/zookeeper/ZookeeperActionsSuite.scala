package com.worekleszczy.zookeeper

import cats.effect.{IO, Resource}
import munit.CatsEffectSuite
import org.testcontainers.containers.GenericContainer
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.log4cats.syntax._
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

class ZookeeperActionsSuite extends CatsEffectSuite {

  private final class ZooKeeperContainer extends GenericContainer[ZooKeeperContainer]("zookeeper:3.8.0")

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

  private val zookeeper =
    ResourceSuiteLocalFixture("zookeeper", Resource.make(startZookeeperResource)(stopZookeeper))

  override lazy val munitFixtures: Seq[Fixture[_]] = List(zookeeper)

  test("test hello world says hi") {
    val test = zookeeper()
    test.getBinds
    info"!_!_!_!_!_!_!_!_!_!_!_!_!_! ${test.getExposedPorts.asScala.map(exposed => test.getMappedPort(exposed)).mkString(", ")}" >> error"Bye bye" >> IO
      .sleep(30.seconds)
  }

}

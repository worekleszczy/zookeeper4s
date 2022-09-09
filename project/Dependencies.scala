import sbt._

object Dependencies {
  object v {
    val catsEffect = "3.3.12"

    val munit          = "1.0.7"
    val zookeeper      = "3.7.0"
    val testcontainers = "1.17.1"
    val log4cats       = "2.3.1"
    val logback        = "1.3.0-alpha16"
  }

  val catsEffect = Seq(
    // "core" module - IO, IOApp, schedulers
    // This pulls in the kernel and std modules automatically.
    "org.typelevel" %% "cats-effect" % v.catsEffect,
    // concurrency abstractions and primitives (Concurrent, Sync, Async etc.)
    "org.typelevel" %% "cats-effect-kernel" % v.catsEffect,
    // standard "effect" library (Queues, Console, Random etc.)
    "org.typelevel" %% "cats-effect-std" % v.catsEffect,
    // better monadic for compiler plugin as suggested by documentation
    "org.typelevel" %% "munit-cats-effect-3" % v.munit % Test
  )

  val fs2 = Seq("co.fs2" %% "fs2-core" % "3.2.12", "co.fs2" %% "fs2-io" % "3.2.12")

  val zookeeper = Seq(
    "org.apache.zookeeper" % "zookeeper" % v.zookeeper
  )

  val testContainers = Seq(
    "org.testcontainers" % "testcontainers" % v.testcontainers % "it",
    "org.testcontainers" % "postgresql"     % v.testcontainers % "it"
  )

  val log4cats = Seq(
    "org.typelevel" %% "log4cats-core"  % v.log4cats,
    "org.typelevel" %% "log4cats-slf4j" % v.log4cats,
    "org.typelevel" %% "log4cats-noop"  % v.log4cats % "test"
  )
  val logback = Seq(
    "ch.qos.logback" % "logback-classic" % v.logback
  )
}

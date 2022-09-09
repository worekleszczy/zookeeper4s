addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.4.6")

addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.10.0")

addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1")

addCompilerPlugin("org.typelevel" % "kind-projector" % "0.13.2" cross CrossVersion.full)

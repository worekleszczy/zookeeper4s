import Dependencies._

val gitHubPackages = "GitHub Package Registry" at "https://maven.pkg.github.com/worekleszczy/zookeeper4s"

inThisBuild(
  List(
    organization := "com.worekleszczy",
    scalaVersion := "2.13.8",
    semanticdbEnabled := true,
    semanticdbVersion := scalafixSemanticdb.revision,
    scalacOptions += "-Wconf:cat=unused:info",
    scalacOptions ++= Seq(
      "-Wconf:cat=unused:info",
      "-encoding",
      "utf8",
      "-feature",
      "-unchecked",
      "-language:existentials",
      "-language:experimental.macros",
      "-language:higherKinds",
      "-language:implicitConversions",
      "-Xcheckinit",
      "-Xlint:adapted-args",
      "-Xlint:constant",
      "-Xlint:delayedinit-select",
      "-Xlint:deprecation",
      "-Xlint:doc-detached",
      "-Xlint:implicit-recursion",
      "-Xlint:implicit-not-found",
      "-Xlint:inaccessible",
      "-Xlint:infer-any",
      "-Xlint:missing-interpolator",
      "-Xlint:nullary-unit",
      "-Xlint:option-implicit",
      "-Xlint:package-object-classes",
      "-Xlint:poly-implicit-overload",
      "-Xlint:private-shadow",
      "-Xlint:stars-align",
      "-Xlint:strict-unsealed-patmat",
      "-Xlint:type-parameter-shadow",
      "-Xlint:-byname-implicit",
      "-Wdead-code",
      "-Wextra-implicit",
      "-Wnumeric-widen",
      "-Wvalue-discard",
      "-Wunused:nowarn",
      "-Wunused:implicits",
      "-Wunused:explicits",
      "-Wunused:imports",
      "-Wunused:locals",
      "-Wunused:params",
      "-Wunused:patvars",
      "-Wunused:privates",
      "-Werror"
    ),
    publishTo := Some(gitHubPackages),
    credentials += Credentials(gitHubPackages.name, "maven.pkg.github.com", "_", sys.env("GITHUB_ACCESS_TOKEN"))
  )
)
val It = config("it").extend(Test)

addCompilerPlugin("org.typelevel" % "kind-projector"     % "0.13.2" cross CrossVersion.full)
addCompilerPlugin("com.olegpy"   %% "better-monadic-for" % "0.3.1")

lazy val root = (project in file("."))
  .overrideConfigs(It)
  .settings(
    inConfig(It)(Defaults.testSettings),
    It / fork := true,
    name := "zookeeper4s",
    version := "0.4.0-SNAPSHOT",
    publishConfiguration := publishConfiguration.value.withOverwrite(true),
    libraryDependencies ++= catsEffect ++ zookeeper ++ testContainers ++ log4cats ++ logback ++ fs2 ++ tagging ++ scodec
  )

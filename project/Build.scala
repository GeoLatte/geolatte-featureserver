import sbt._
import Keys._
import play.Project._

object GeolatteNoSqlBuild extends Build {

  val appName = "geolatte-nosql"
  val appVersion = "1.2"

  //Resolvers
  lazy val commonResolvers = Seq(
    "Local Maven Repository" at Path.userHome.asFile.toURI.toURL + "/.m2/repository",
    "Codahale Repo" at "http://repo.codahale.com",
    "Sonatype Repo" at "https://oss.sonatype.org/content/repositories/releases/",
    "Sonatype Snapshots" at "http://oss.sonatype.org/content/repositories/snapshots/",
    "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
    "sbt-idea-repo" at "http://mpeltonen.github.com/maven/",
    Resolver.url("artifactory", url("http://scalasbt.artifactoryonline.com/scalasbt/sbt-plugin-releases"))(Resolver.ivyStylePatterns)
  )

  lazy val coreDependencies = Seq(
    "org.geolatte" % "geolatte-geom" %  "0.14",
    "commons-codec" % "commons-codec" % "1.8",
    "net.sf.supercsv" % "super-csv" % "2.1.0",
    "nl.grons" %% "metrics-scala" % "3.0.4",
    "org.parboiled" %% "parboiled" % "2.0.1",
    filters
  )

  lazy val mongoDependencies = Seq(
    "org.reactivemongo" %% "reactivemongo" % "0.10.0",
    "org.reactivemongo" %% "play2-reactivemongo" % "0.10.2"
  )

  lazy val psqlDependencies = Seq(
    "com.github.mauricio" %% "postgresql-async" % "0.2.15"
  )

  lazy val testDependencies = Seq(
    "org.specs2" %% "specs2" % "2.4.1" % "test"
  )


  //Dependencies
  lazy val dependencies = coreDependencies ++
    mongoDependencies ++
    psqlDependencies ++
    testDependencies

  //Build Settings applied to all projects
  lazy val commonBuildSettings = Seq(
    organization := "org.geolatte.nosql",
    scalaVersion := "2.10.4",
    scalacOptions ++= Seq("-feature", "-language:postfixOps", "-language:implicitConversions"),
    resolvers ++= commonResolvers
  )

  lazy val ItTest = config("integration") extend Test

  def itFilter(name: String): Boolean = name startsWith "integration"
  def unitFilter(name: String): Boolean = !itFilter(name)

  //Settings applied to all projects
  lazy val defaultSettings =
    commonBuildSettings ++
      Seq(
        libraryDependencies ++= dependencies,
        javaOptions in(Test, run) += "-XX:MaxPermSize=128m -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005",
        javacOptions ++= Seq("-source", "1.6", "-target", "1.6")
      )

  //Options for running tests
  val testSettings = Seq(
    Keys.fork in Test := false, //Fork a new JVM for running tests
    testOptions in Test := Seq(Tests.Filter(unitFilter)),
    parallelExecution in ItTest := false,
    testOptions in ItTest := Seq(Tests.Argument("sequential"), Tests.Filter(itFilter))
  )

  val main = play.Project(
    appName,
    appVersion,
    dependencies = dependencies
  ).configs(ItTest)
    .settings(inConfig(ItTest)(Defaults.testTasks): _*)
    .settings((defaultSettings ++ testSettings): _*)

}

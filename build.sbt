scalaVersion := "2.13.16"

val appName = "geolatte-featureserver"
val appVersion = "2.0-SNAPSHOT"

//Resolvers
lazy val commonResolvers = Seq(
  "Local Maven Repository" at Path.userHome.asFile.toURI.toURL + "/.m2/repository",
  "Sonatype Repo" at "https://oss.sonatype.org/content/repositories/releases/",
  "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/"
  )

lazy val coreDependencies = Seq(
  "org.geolatte" % "geolatte-geom" % "0.14",
//  "commons-codec" % "commons-codec" % "1.8",
  "net.sf.supercsv" % "super-csv" % "2.4.0",
  "org.parboiled" %% "parboiled" % "2.5.1",
  "org.apache.pekko" %% "pekko-slf4j" % play.core.PlayVersion.pekkoVersion,
  "net.logstash.logback" % "logstash-logback-encoder" % "7.3",
  filters,
  guice
  )

lazy val slickVersion = "3.3.2"

lazy val psqlDependencies = Seq(
  "com.typesafe.slick" %% "slick" % slickVersion,
  "com.typesafe.slick" %% "slick-hikaricp" % slickVersion,
  "org.postgresql" % "postgresql" % "42.7.8"
  )

lazy val prometheusClientVersion = "0.8.0"

lazy val prometheusDependencies = Seq(
  "io.prometheus" % "simpleclient" % prometheusClientVersion,
  "io.prometheus" % "simpleclient_common" % prometheusClientVersion,
  "io.prometheus" % "simpleclient_hotspot" % prometheusClientVersion,
  "io.prometheus" % "simpleclient_dropwizard" % prometheusClientVersion
  )

lazy val testDependencies = Seq(
  specs2 % Test
  )


//Dependencies
lazy val dependencies = coreDependencies ++
  psqlDependencies ++
  prometheusDependencies ++
  testDependencies

//Build Settings applied to all projects
lazy val commonBuildSettings = Seq(
  name := appName,
  version := appVersion,
  organization := "org.geolatte",
  scalacOptions ++= Seq( "-feature", "-language:postfixOps", "-language:implicitConversions" , "-deprecation"),
  resolvers ++= commonResolvers
  )

lazy val ItTest = config( "integration" ) extend Test

def itFilter(name: String): Boolean = name.startsWith("integration")

def unitFilter(name: String): Boolean = !itFilter( name )

//Settings applied to all projects
lazy val defaultSettings =
  commonBuildSettings ++
    Seq(
      libraryDependencies ++= dependencies,
      run / fork := true
      )

//Options for running tests
val testSettings = Seq(
  Test / fork := true, //Fork a new JVM for running tests
  Test / testOptions := Seq( Tests.Filter( unitFilter ) ),
  ItTest / parallelExecution := false,
  ItTest / testOptions := Seq( Tests.Argument( "sequential" ), Tests.Filter( itFilter ) )
  )

val `geolatte-geoserver` = project
  .in(file("."))
  .settings(defaultSettings)
  .configs( ItTest )
  .settings(inConfig( ItTest )( Defaults.testTasks ))
  .settings(defaultSettings ++ testSettings)
  .enablePlugins(PlayScala)

import sbt._
import Keys._
import play.Project._

object GeolatteNoSqlBuild extends Build {

   val appName         = "geolatte-nosql"
   val appVersion      = "1.1"

  //Resolvers
  lazy val commonResolvers = Seq(
    "Local Maven Repository" at Path.userHome.asFile.toURI.toURL +"/.m2/repository",
    "Codahale Repo" at "http://repo.codahale.com",
    "Sonatype Repo" at "https://oss.sonatype.org/content/repositories/releases/",
    "Sonatype Snapshots" at "http://oss.sonatype.org/content/repositories/snapshots/",
    "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",    
    "sbt-idea-repo" at "http://mpeltonen.github.com/maven/",
    Resolver.url("artifactory", url("http://scalasbt.artifactoryonline.com/scalasbt/sbt-plugin-releases"))(Resolver.ivyStylePatterns)
  )

   //Dependencies
  lazy val dependencies = Seq(
    "org.specs2" %% "specs2" % "2.3.4" % "test",
    "org.geolatte" % "geolatte-geom" %  "0.12",
    "org.reactivemongo" %% "reactivemongo" % "0.10.0",
     "org.reactivemongo" %% "play2-reactivemongo" % "0.10.2",
    "commons-codec" % "commons-codec" % "1.8",
    "net.sf.supercsv" % "super-csv" % "2.1.0",
     "nl.grons" %% "metrics-scala" % "3.0.4",
    filters
  )

  //Build Settings applied to all projects
  lazy val commonBuildSettings = Seq(
    organization := "org.geolatte.nosql",
    scalaVersion := "2.10.2",
    scalacOptions ++= Seq("-feature", "-language:postfixOps", "-language:implicitConversions"),
    resolvers ++= commonResolvers
  )

  //Settings applied to all projects
  lazy val defaultSettings =
  		commonBuildSettings ++
  		Seq(
    		libraryDependencies ++= dependencies,
    		javaOptions in (Test,run) += "-XX:MaxPermSize=128m -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005",
        javacOptions ++= Seq("-source", "1.6", "-target", "1.6")
  		)

  //Options for running tests
  val testSettings = Seq (
    Keys.fork in Test := false,  //Fork a new JVM for running tests
    parallelExecution in Test := false,
    testOptions in Test += Tests.Argument("sequential")
  )

  val main = play.Project(
    	appName,
      appVersion,
    	dependencies = dependencies
  ).settings( (defaultSettings ++ testSettings):_*)

}

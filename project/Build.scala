import sbt._
import Keys._
import sbtbuildinfo.Plugin._
import sbtrelease._
import ReleasePlugin._
import ReleaseKeys._
import ReleaseStateTransformations._
import sbtassembly.Plugin._
import sbtassembly.Plugin.AssemblyKeys._

object GeolatteNoSqlBuild extends Build {

   fork := true

   val appName         = "geolatte-nosql"
   val appVersion      = "0.1"

  //Resolvers
  lazy val commonResolvers = Seq(
    "Local Maven Repository" at "file://"+Path.userHome+"/.m2/repository",
    "Codahale Repo" at "http://repo.codahale.com",
    "Sonatype Repo" at "https://oss.sonatype.org/content/repositories/releases/",
    "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",    
    "sbt-idea-repo" at "http://mpeltonen.github.com/maven/",
    Resolver.url("artifactory", url("http://scalasbt.artifactoryonline.com/scalasbt/sbt-plugin-releases"))(Resolver.ivyStylePatterns)
  )

   //Dependencies
  lazy val dependencies = Seq(
    "org.specs2" %% "specs2" % "1.14" % "test",    
    "org.geolatte" % "geolatte-geom" %  "0.12-SNAPSHOT",
    "org.reactivemongo" %% "reactivemongo" % "0.9",
     "org.reactivemongo" %% "play2-reactivemongo" % "0.9",
    "org.geolatte" % "geolatte-common" % "0.7-SNAPSHOT"
      exclude("org.codehaus.jackson", "jackson-core-lgpl")     //needs to exclude these because they conflict with Play's more recent Jackson
      exclude("org.codehaus.jackson", "jackson-mapper-lgpl")

  )

  //Build Settings applied to all projects
  lazy val commonBuildSettings = Seq(
    organization := "org.geolatte.nosql",
    scalaVersion := "2.10.0",
    scalacOptions += "-feature",
    resolvers ++= commonResolvers
  )

  //Settings applied to all projects
  lazy val defaultSettings = Defaults.defaultSettings ++ 
  		play.Project.defaultScalaSettings ++ 
  		assemblySettings ++
  		commonBuildSettings ++ 
  		releaseSettings ++ 
  		Seq(
    		libraryDependencies ++= dependencies,
    		releaseProcess := releaseSteps,
    		fork in test := true,  //Fork a new JVM for running tests
    		javaOptions in (Test,run) += "-XX:MaxPermSize=128m"
  		)
  
  //Release steps for sbt-release plugin
  lazy val releaseSteps = Seq[ReleaseStep](
    //checkSnapshotDependencies,              // Check whether the working directory is a git repository and the repository has no outstanding changes
    //inquireVersions,                        // Ask the user for the release version and the next development version
    runTest                                // Run test:test, if any test fails, the release process is aborted
    //setReleaseVersion,                      // Write version in ThisBuild := "releaseVersion" to the file version.sbt and also apply this setting to the current build state.
    //commitReleaseVersion                   // Commit the changes in version.sbt.
    //tagRelease                             // Tag the previous commit with version (eg. v1.2, v1.2.3).
    //publishArtifacts,                       // Run publish.
    //setNextVersion,                         // Write version in ThisBuild := "nextVersion" to the file version.sbt and also apply this setting to the current build state.
    //commitNextVersion,                      // Commit the changes in version.sbt.
    //pushChanges                             // Push changes
  )

    lazy val main = play.Project(
    	appName,
    	dependencies = dependencies,
    	settings = defaultSettings ++ buildInfoSettings
  	)

}

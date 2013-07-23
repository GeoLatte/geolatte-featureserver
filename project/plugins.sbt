// Comment to get more information during initialization
logLevel := Level.Warn

// Use the Play sbt plugin for Play project

resolvers += "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/"

addSbtPlugin("play" % "sbt-plugin" % "2.1.0")

addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.2.0")

addSbtPlugin("com.github.gseitz" % "sbt-release" % "0.6")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.8.6")



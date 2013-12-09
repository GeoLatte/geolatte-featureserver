// Comment to get more information during initialization
logLevel := Level.Warn

// Use the Play sbt plugin for Play project

resolvers += "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/"

addSbtPlugin("com.typesafe.play" % "sbt-plugin" % "2.2.1")

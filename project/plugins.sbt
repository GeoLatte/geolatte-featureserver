// Comment to get more information during initialization
logLevel := Level.Warn

resolvers += "Typesafe repository" at "https://repo.typesafe.com/typesafe/releases/"

// Use the Play sbt plugin for Play project
addSbtPlugin("com.typesafe.play" % "sbt-plugin" % "2.5.6")

//Scalariform plugin
addSbtPlugin("org.scalariform" % "sbt-scalariform" % "1.6.0")


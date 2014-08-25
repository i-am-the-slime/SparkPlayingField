addSbtPlugin("com.github.mpeltonen" % "sbt-idea" % "1.6.0")

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.7.4")

//resolvers += "sbt-plugin-releases" at "http://repo.scala-sbt.org/scalasbt/sbt-plugin-releases"

//addSbtPlugin("com.cavorite" % "sbt-avro" % "0.3.2")


//"Macro annotations are only available in Scala 2.10.x and 2.11.x with the macro paradise plugin. Their inclusion in official Scala might happen in Scala 2.12 - official [docs](http://docs.scala-lang.org/overviews/macros/annotations.html)"
addCompilerPlugin("org.scalamacros" % "paradise" % "2.0.0" cross CrossVersion.full)
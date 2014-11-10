import sbtassembly.Plugin.AssemblyKeys
import AssemblyKeys._

assemblySettings

run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) => {
  case PathList(ps @ _*) if ps.last endsWith ".RSA" => MergeStrategy.discard
  case x if x startsWith "META-INF/" => MergeStrategy.discard
  //case "plugin.properties" => MergeStrategy.discard
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
  case PathList("plugin.properties") => MergeStrategy.last
  case "application.conf" => MergeStrategy.concat
  case "unwanted.txt"     => MergeStrategy.discard
  case x => old(x)
//  case PathList(ps @ _*) if ps.last endsWith "Log$Logger.class" => MergeStrategy.first
//  case PathList(ps @ _*) if ps.last endsWith "Log.class" => MergeStrategy.first
//  case PathList(ps @ _*) if ps.last endsWith "BasicDynaBean.class" => MergeStrategy.last
//  case PathList(ps @ _*) if ps.last endsWith "BasicDynaClass.class" => MergeStrategy.last
}}

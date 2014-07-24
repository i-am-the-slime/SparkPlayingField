import sbtassembly.Plugin.AssemblyKeys
import AssemblyKeys._

assemblySettings

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) => {
  case PathList(ps @ _*) if ps.last endsWith ".RSA" => MergeStrategy.discard
  case x if x startsWith "META-INF/" => MergeStrategy.discard
  case "plugin.properties" => MergeStrategy.discard
//  case PathList(ps @ _*) if ps.last endsWith "Log$Logger.class" => MergeStrategy.first
//  case PathList(ps @ _*) if ps.last endsWith "Log.class" => MergeStrategy.first
//  case PathList(ps @ _*) if ps.last endsWith "BasicDynaBean.class" => MergeStrategy.last
//  case PathList(ps @ _*) if ps.last endsWith "BasicDynaClass.class" => MergeStrategy.last
  case x => old(x)
}
}

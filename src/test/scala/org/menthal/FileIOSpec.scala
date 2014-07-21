package org.menthal

import org.menthal.model.events.CCAppInstall
import org.scalatest.{FlatSpec, Matchers}

class FileIOSpec extends FlatSpec with Matchers {
  "The FileIO class" should "read and write RDDs of AppSession" in {
    val sc = SparkTestHelper.getLocalSparkContext
    val data = sc.parallelize(Seq(CCAppInstall(1,2,3, "appName", "pkgName")))
//    FileIO.write(data)
  }
}

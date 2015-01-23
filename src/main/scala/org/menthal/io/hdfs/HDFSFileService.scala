package org.menthal.io.hdfs

/**
 * Created by konrad on 23.01.15.
 */

import java.io.BufferedInputStream
import java.io.File
import java.io.FileInputStream
import java.io.InputStream
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._

import scala.util.{Try, Random}

object HDFSFileService {

  private val conf = new Configuration

  private val fileSystem = FileSystem.get(conf)

  def saveFile(filepath: String): Unit = {
    val file = new File(filepath)
    val out = fileSystem.create(new Path(file.getName))
    val in = new BufferedInputStream(new FileInputStream(file))
    var b = new Array[Byte](1024)
    var numBytes = in.read(b)
    while (numBytes > 0) {
      out.write(b, 0, numBytes)
      numBytes = in.read(b)
    }
    in.close()
    out.close()
  }

  def removeFile(filename: String): Boolean = {
    val path = new Path(filename)
    fileSystem.delete(path, false)
  }

  def getFile(filename: String): InputStream = {
    val path = new Path(filename)
    fileSystem.open(path)
  }

  def removeDir(dirname:String): Boolean = {
    val path = new Path(dirname)
    fileSystem.delete(path, true)
  }

  def exists(filename:String):Boolean = {
    val path = new Path(filename)
    fileSystem.exists(path)
  }

  def createFolder(folderPath: String): Boolean = {
    val path = new Path(folderPath)
    fileSystem.mkdirs(path)
  }

  def createTmp(root: String = "/tmp"):Option[String] = {
    val folderPath = root + "/" + Random.nextString(8)
    if (createFolder(folderPath))
      return Some(folderPath)
    else None
  }

  def createTmps(n: Int, root: String = "/tmp"):List[String] = {
    (1 to n) flatMap (_ => createTmp(root)) toList
  }

  def rename(originalPath:String, newPath: String) = {
    val src = new Path(originalPath)
    val dest = new Path(newPath)
    fileSystem.rename(src, dest)
  }

  def forceRename(originalPath:String, newPath: String) = {
    if (exists(newPath))
      removeDir(newPath)
    val src = new Path(originalPath)
    val dest = new Path(newPath)
    fileSystem.rename(src, dest)
  }
}

package com.urdnot.directoryWatch

import java.net.URI

import akka.actor.ActorSystem
import org.apache.hadoop.fs._
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.conf.Configuration

object directoryWatcher extends App {
  val system: ActorSystem = ActorSystem("directoryWatcher")

  val files = getListOfFiles("", "", new Path(""), new Path(""), "", new URI(""))

  def getListOfFiles(hdfsUser: String,
                     hdfsKeyTab: String,
                     sitePath: Path,
                     hdfsPath: Path,
                     dir: String,
                     uri: URI):
  List[String] = {
    UserGroupInformation.loginUserFromKeytab(hdfsUser, hdfsKeyTab)
    val hdfsConf = new Configuration()
    hdfsConf.addResource(sitePath)
    hdfsConf.addResource(hdfsPath)
    hdfsConf.set("hadoop.security.authentication", "Kerberos")
    val fs = FileSystem.get(uri, hdfsConf)
    fs.listFiles(new Path(dir), true)


    /*val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.map(file => file.getName).toList
    } else {
      List[String]()
    }*/
  }
}

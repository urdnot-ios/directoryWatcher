package com.urdnot.directoryWatch

import java.net.URI

import akka.actor.ActorSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.security.UserGroupInformation

object directoryWatcher extends App {
  if (args.length == 0) System.exit(1)
  val system: ActorSystem = ActorSystem("directoryWatcher")
  /*
  args(0) = "jsewell10@HDPQUANTUMSTG.COM"
  args(1) = "/etc/security/keytabs/jsewell10.user.keytab"
  args(2) = "/home/jsewell10/config/core-site.xml"
  args(3) = "/home/jsewell10/config/hdfs-site.xml"
  args(4) = new URI("hdfs://pxnhd")
  args(5) = "/user/cdrbd/unification/orc/cdr-voice/date_part=20170920/feed_detail_id_part=2/"
  /home/jsewell10/bin/scala-2.12.1/bin/scala -cp $(hadoop classpath):/home/jsewell10/lib/config-1.3.1.jar:/home/jsewell10/lib/akka-actor_2.12-2.5.3.jar:/home/jsewell10/bin/directorywatcher_2.12-0.1.jar \
  com.urdnot.directoryWatch.directoryWatcher jsewell10@HDPQUANTUMSTG.COM /etc/security/keytabs/jsewell10.user.keytab /home/jsewell10/config/core-site.xml /home/jsewell10/config/hdfs-site.xml hdfs://pxnhd /user/cdrbd/unification/orc/cdr-voice/date_part=20170920/feed_detail_id_part=2/
   */
  val files = getListOfFiles(args(0), args(1), new Path(args(2)), new Path(args(3)), new URI(args(4)), args(5))
  println("file count: " + files.size)

  files.foreach(x => println("path: " + x._1 + "\tmod : " + x._2 + "\tlen : " + x._3))


  def getListOfFiles(hdfsUser: String,
                     hdfsKeyTab: String,
                     sitePath: Path,
                     hdfsPath: Path,
                     uri: URI,
                     dir: String):
  List[(Path, String, String)] = {
    UserGroupInformation.loginUserFromKeytab(hdfsUser, hdfsKeyTab)
    val hdfsConf = new Configuration()
    hdfsConf.addResource(sitePath)
    hdfsConf.addResource(hdfsPath)
    hdfsConf.set("hadoop.security.authentication", "Kerberos")
    val fs = FileSystem.get(uri, hdfsConf)
    val fileIterator = fs.listFiles(new Path(dir), true)
    buildFileList(fileIterator)
  }
  def buildFileList(iter: RemoteIterator[LocatedFileStatus]): List[(Path, String, String)] = {
    def loop(iter: RemoteIterator[LocatedFileStatus], acc: List[(Path, String, String)]): List[(Path, String, String)] = {
      if (iter.hasNext) {
        val fileStatus = iter.next
        loop(iter, (fileStatus.getPath, fileStatus.getModificationTime.toString, fileStatus.getLen.toString) :: acc)
      } else {
        acc
      }
    }
    loop(iter, List.empty[(Path, String, String)])
  }
  /*
  LocatedFileStatus{path=hdfs://pxnhd/user/cdrbd/unification/orc/cdr-voice/date_part=20170920/feed_detail_id_part=2/node_part=ATMSS344/000019_0;
  isDirectory=false;
  length=265967649;
  replication=3;
  blocksize=268435456;
  modification_time=1506060214574;
           date -d @1506060214
           Thu Sep 21 23:03:34 PDT 2017
  access_time=1506060202137;
  owner=cdrbd; group=hadoop; permission=rwxr-xr-x;
  isSymlink=false}
*/


}

import java.util.Date

import area.GenhelinyejuAreaList
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.broadcast.Broadcast
import utils.{HbaseUtilNew, TimeFunc}

import scala.collection.mutable
import scala.io.Source

object test extends TimeFunc {
  def main(args: Array[String]): Unit = {
    val elunchungonganju = List("18187-102072994", "18187-102072985", "18187-102072983", "18187-102072984", "18187-102072963", "18187-104110999", "18187-104111001", "18187-104111000","18187-104110999","18187-104111000","18187-104111001","18187-101876887","18187-101876888","18187-101876889","18187-101966743","18187-101966744","18187-101966745","18187-24091","18187-24092","18187-101876867","18187-24111","18187-24112")

    println(elunchungonganju.size)
    println(elunchungonganju.toSet.size)



  }

}

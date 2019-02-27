import java.util.Date

import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.broadcast.Broadcast
import utils.{HbaseUtilNew, TimeFunc}

import scala.collection.mutable
import scala.io.Source

object test extends TimeFunc {
  def main(args: Array[String]): Unit = {
    //    val startTime="2018-06-28 12:00:12"
    val hunValue = new HbaseUtilNew
    val conn=hunValue.createHbaseConnection
    val distinctPartition=List("123456")
    hunValue.getResultByKeyList_Leader(conn, "TourMasLeaderUser", "0", distinctPartition)
      .foreach(println)


  }

}

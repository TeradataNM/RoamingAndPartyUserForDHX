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
    val conn = hunValue.createHbaseConnection
    val distinctPartition = List("15904983903", "15193790206", "15193790206", "15193790206", "15040135308", "15040135308", "15040135308", "13853116409", "13853116409", "13853116409", "15934922211", "15934922211", "15934922211", "18774737311", "18774737311", "18774737311", "15810633012", "15810633012", "15810633012", "15847746612", "15847746612", "15847746612", "13404701413", "13404701413", "13404701413", "13470921116", "13470921116", "13470921116", "15134762517", "15134762517", "15134762517", "13847921118", "13847921118", "13847921118", "15148699518", "15148699518", "15148699518", "15204242819", "15204242819").distinct

//    val resList=hunValue.getResultByFilter("TourMasUsualUser")
//    resList.foreach(println)
    val list=List("15754861048")
//    hunValue.getResultByKeyList_USER(conn,"TourMasUsualUser",list)

    val chifengLeaders = hunValue.getAllRows("TourMasLeaderUser", "0", "flag")
    chifengLeaders.foreach(println)

  }

}

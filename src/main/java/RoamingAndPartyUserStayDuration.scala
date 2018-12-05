
import java.util.{Calendar, Properties}

import kafka.serializer.StringDecoder
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import utils._

import scala.collection.mutable

/**
  * Created by yaodongzhe on 2017/7/11.
  */
object RoamingAndPartyUserStayDuration extends TimeFunc with Serializable {


  var lastTime = Calendar.getInstance().getTime
  val timeFreq: Long = 300000L

  def main(args: Array[String]) {
    val sparkConfig = new SparkConfig
    val conf = sparkConfig.getConf.setAppName("RoamingAndPartyUserForDHX")
    val ssc = new StreamingContext(conf, Seconds(120))
    val hun = new HbaseUtilNew
    val hunBro = ssc.sparkContext.broadcast(hun)

    val gansu_ningxia_haoduan_bro: Broadcast[Set[String]] =
      ssc.sparkContext.broadcast(
        ssc.sparkContext.textFile("hdfs://nmdsj133nds/user/yaodongzhe/gansu_ningxia_haoduan_file")
          .map(_.trim).toArray().toSet)


    val topicSet = Array(Set("O_DPI_LTE_S1_MME"), Set("O_DPI_MC_LOCATIONUPDATE_2G"), Set("O_DPI_MC_LOCATIONUPDATE_3G"))

    val kafkaParams = Map[String, String](
      "bootstrap.servers" -> "SS-B02-M20-G03-X3650M5-6:6667,SS-B02-M20-G03-X3650M5-7:6667,SS-B02-M20-G04-X3650M5-1:6667",
      "group.id" -> "RoamingAndPartyUserForDHX",
      "zookeeper.connection.timeout.ms" -> "100000"
    )


    val numStreams = 3
    val kafkaStreams = (1 to numStreams).map { i => KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicSet(i - 1)) }
    val unifiedStream = ssc.union(kafkaStreams)


    val kafkaProducer: Broadcast[KafkaSink[String, String]] = {
      val kafkaProducerConfig = {
        val p = new Properties()
        p.setProperty("bootstrap.servers", "10.221.156.226:9092,10.221.156.227:9092,10.221.156.228:9092,10.221.156.229:9092,10.221.156.230:9092,10.221.156.231:9092")
        p.setProperty("key.serializer", classOf[StringSerializer].getName)
        p.setProperty("value.serializer", classOf[StringSerializer].getName)
        p
      }
      ssc.sparkContext.broadcast(KafkaSink[String, String](kafkaProducerConfig))
    }


    val filterStream = unifiedStream.map(m => {
      m._2.split("\\|", -1)
    }).map((m: Array[String]) => {
      if (m.length == 1) {
        m(0).split(",", -1)
      } else m
    }).filter((f: Array[String]) => {
      if (f.length == 211) {
        if (f(11).nonEmpty) {
          true
        } else false
      } else if (f.length == 74 || f.length == 75) {
        if (f(18).nonEmpty) {
          true
        } else false
      } else false
    }).map(m => {
      //    业务流程开始时间，手机号，imei,lac,cell,上行流量，下行流量，exp9
      var start_time, phone_no, lac, cell, local_city, owner_province, owner_city, roam_type = ""
      var start_time_long = 0L
      if (m.length == 211) {

        start_time_long = m(13).toLong
        start_time = timeMillsToDate(start_time_long, "yyyy-MM-dd HH:mm:ss")

        phone_no = m(11).replaceAll("^86", "")
        lac = m(43)
        cell = m(44)
        roam_type = m(5)
        local_city = m(2)
        owner_province = m(3)
        owner_city = m(4)


      } else {
        start_time = m(0).substring(0, 19)
        start_time_long = date2TimeStamp(start_time, format = "yyyy-MM-dd HH:mm:ss").toLong
        phone_no = m(18).replaceAll("^86", "")
        //lac cell 未知
        lac = m(29)
        cell = m(30)
        local_city = ""
        roam_type = m(73)
        owner_province = m(71)
        owner_city = m(72)

      }
      (phone_no, ((start_time, start_time_long), phone_no, local_city, roam_type, owner_province, owner_city, lac, cell))

    })


    filterStream.foreachRDD(rdd => {


      rdd.partitionBy(new HashPartitioner(partitions = 200)).foreachPartition(partition => {
        //          kafka-topics.sh --zookeeper zk-01:2181,zk-02:2181,zk-03:2181/kafka --create --replication-factor 1 --partitions 7 --topic ROAMING-AND-PARTY-USER-FOR-DHX
        val targetTopic = "ZNDX-USER-FOR-DHX"
        val area = new AreaList

        val hunValue = hunBro.value
        val conn = hunValue.createHbaseConnection
        val badanjilin = area.badanjilin
        val elunchun = area.elunchun
        val gansu_ningxia_haoduan = gansu_ningxia_haoduan_bro.value
        val honghuaerji = area.honghuaerji
        val wulanbuhe = area.wulanbuhe
        val tuoxian = area.tuoxian


        val sortedPartition = partition.toList.sortBy(_._2._1._2)

        val distinctPartition: List[String] = sortedPartition.map(_._1).distinct

        val lastUserStatus = hunValue.getResultByKeyList_MAS(conn, "TourMasUser", distinctPartition)

        //        鄂伦春常驻用户离开情况
        //        val elunchunPermanentPopulation: mutable.HashMap[String, String] =
        //          hunValue.getResultByKeyList_USER(conn, "TourMasUsualUser", distinctPartition)

        //        val elunchunLeaveUser = elunchunPermanentPopulation.filter(_._2.equals("1"))
        //        val elunchunNotLeaveUser = elunchunPermanentPopulation.filter(_._2.equals("0"))

        //        离开过赤峰的领导
        val chifengLeaders: mutable.HashMap[String, String] =
          hunValue.getResultByKeyList_Leader(conn, "TourMasLeaderUser", "0", "flag", distinctPartition)

        val chifengLeaveLeaders = chifengLeaders.filter(_._2.equals("1"))
        val chifengNotLeaveLeaders = chifengLeaders.filter(_._2.equals("0"))

        //        val newElunchunPutMap: mutable.HashMap[String, String] = new scala.collection.mutable.HashMap
        val newChifengLeaderPutMap: mutable.HashMap[String, String] = new scala.collection.mutable.HashMap


        sortedPartition.foreach(kline => {

          val line: ((String, Long), String, String, String, String, String, String, String) = kline._2

          val phone_no = line._2
          val local_city = line._3
          val roam_type = line._4
          val owner_province = line._5
          val owner_city = line._6
          val lac = line._7
          val ci = line._8
          val lac_ci = lac + "-" + ci
          val startTime: String = line._1._1
          val startTimeLong: Long = line._1._2


          val stringLine = startTime + "|" +
            phone_no + "|" +
            local_city + "|" +
            roam_type + "|" +
            owner_province + "|" +
            owner_city + "|" +
            lac + "|" +
            ci

          val chifengFunc: Boolean = {
            if (!roam_type.equals("4")
              && !roam_type.equals("")
              && local_city.equals("0476")
              && !owner_city.equals("0476")) true else false
          }

          val elunchunFunc: Boolean = {
            if (
            //              如果用户是漫入到鄂伦春的
              !roam_type.equals("4")
                && !roam_type.equals("")
                && local_city.equals("0470")
                && !owner_city.equals("0470")
                && elunchun.contains(lac_ci)

            ) true else false
          }
          //          val elunchunFunc1: Boolean = {
          //            if (
          //            //              如果用户是漫入到鄂伦春的
          //              !roam_type.equals("4")
          //                && !roam_type.equals("")
          //                && local_city.equals("0470")
          //                && !owner_city.equals("0470")
          //                && elunchun.contains(lac_ci)
          //
          //            ) true else false
          //          }

          //          val elunchunFunc2: Boolean = {
          //            if (
          //            //                或者如果离开过鄂伦春的用户包含此用户 且 此用户重新进入了鄂伦春
          //              elunchunLeaveUser.contains(phone_no)
          //                && elunchun.contains(lac_ci)
          //            ) true else false
          //          }

          val tongliaoFunc: Boolean = {
            if (!roam_type.equals("4")
              && !roam_type.equals("")
              && local_city.equals("0475")
              && !owner_city.equals("0475")) true else false
          }


          val badanjilinFunc: Boolean = {
            var successFlag = false
            var phone_no_head = ""
            try {
              if (phone_no.startsWith("1064")) {
                phone_no_head = phone_no.substring(0, 9)
              }
              else {
                phone_no_head = phone_no.substring(0, 7)
              }

              if ((roam_type.equals("1") || roam_type.equals("2"))
                && badanjilin.contains(lac_ci)
                && !gansu_ningxia_haoduan.contains(phone_no_head)

              ) successFlag = true
              else {
                successFlag = false
              }
            }
            catch {
              case e => println("badanjilinFunc,error phoneno is:" + phone_no)
            }
            successFlag
          }

          val eerduosiFunc = {
            if (!roam_type.equals("4")
              && !roam_type.equals("")
              && local_city.equals("0477")
              && !owner_city.equals("0477")) true else false
          }

          val honghuaerjiFunc = {
            if (honghuaerji.contains(lac_ci)) true else false
          }

          val wulanbuheFunc = {
            if (wulanbuhe.contains(lac_ci)
              && !roam_type.equals("4")
              && !roam_type.equals("")
              && local_city.equals("0483")
              && !owner_city.equals("0483")
            ) true else false
          }

          val tuoxianFunc = {
            if (tuoxian.contains(lac_ci)
              && !roam_type.equals("4")
              && !roam_type.equals("")
              && local_city.equals("0471")
              && !owner_city.equals("0471")
            ) true else false
          }

          val huhehaoteFunc = {
            if (!roam_type.equals("4")
              && !roam_type.equals("")
              && local_city.equals("0471")
              && !owner_city.equals("0471")
            ) true else false
          }


          if (lastUserStatus.contains(phone_no)) {

            val lastStatus: (String, Long, Long) = lastUserStatus(phone_no)
            val lastEventType = lastStatus._1

            def judgeUserStayDuration(areaFunc: Boolean
                                      , timeout: Long
                                     ) {

              if (areaFunc) {
                //                    用户上批次驻留时间(秒)
                val lastDuration = lastStatus._3
                //                    用户上批次驻留开始时间(秒)
                val lastStartTime = lastStatus._2
                //                  startTimeLong(毫秒)/1000 -lastStartTime(秒)+lastDuration(秒)
                val newDuration = startTimeLong / 1000 - lastStartTime + lastDuration
                //                    当此用户驻留时间超过1个小时
                if (newDuration >= timeout) {
                  kafkaProducer.value.send(targetTopic, lastEventType + "|" + stringLine)

                  //                  如果鄂伦春用户发送过信息，将鄂伦春用户重置回未离开过
                  //                  if (elunchunFunc2) {
                  //                    newElunchunPutMap.update(phone_no, "0")
                  //                  }
                }

                lastUserStatus.update(phone_no, (lastEventType, startTimeLong / 1000, newDuration))
              } else {
                lastUserStatus.update(phone_no, ("X", startTimeLong / 1000, 0))
              }

            }


            //            漫入赤峰人群(所在地市，漫游类型)id:2
            //              赤峰2
            if (lastEventType.equals("2")) {

              judgeUserStayDuration(chifengFunc, 3600L)
            }
            //            鄂伦春11
            else if (lastEventType.equals("11")) {
              judgeUserStayDuration(elunchunFunc, 600L)
            }
            //              通辽9
            else if (lastEventType.equals("9")) {
              judgeUserStayDuration(tongliaoFunc, 1800L)
            }
            //            巴丹吉林旅游区14
            else if (lastEventType.equals("14")) {
              judgeUserStayDuration(badanjilinFunc, 3600L)
            }
            //              鄂尔多斯7
            else if (lastEventType.equals("7")) {
              judgeUserStayDuration(eerduosiFunc, 1800L)
            }
            //              红花尔基15
            else if (lastEventType.equals("15")) {
              judgeUserStayDuration(honghuaerjiFunc, 1800L)
            }
            //            阿拉善乌兰布和23
            else if (lastEventType.equals("23")) {
              judgeUserStayDuration(wulanbuheFunc, 3600L)
            }

            //              托县25
//            else if (lastEventType.equals("25")) {
//              judgeUserStayDuration(tuoxianFunc, 600L)
//            }
            //              呼和浩特27
//            else if (lastEventType.equals("27")) {
//              judgeUserStayDuration(huhehaoteFunc, 60L)
//            }


          }
          else {
            //                漫入赤峰，更新临时列表
            if (chifengFunc) {
              lastUserStatus.update(phone_no, ("2", startTimeLong / 1000, 0))
            }
            else if (elunchunFunc) {
              lastUserStatus.update(phone_no, ("11", startTimeLong / 1000, 0))
            }
            else if (tongliaoFunc) {
              lastUserStatus.update(phone_no, ("9", startTimeLong / 1000, 0))
            }
            else if (badanjilinFunc) {
              lastUserStatus.update(phone_no, ("14", startTimeLong / 1000, 0))
            }
            else if (eerduosiFunc) {
              lastUserStatus.update(phone_no, ("7", startTimeLong / 1000, 0))
            }
            else if (honghuaerjiFunc) {
              lastUserStatus.update(phone_no, ("15", startTimeLong / 1000, 0))
            }
            else if (wulanbuheFunc) {
              lastUserStatus.update(phone_no, ("23", startTimeLong / 1000, 0))
            }
//            else if (tuoxianFunc) {
//              lastUserStatus.update(phone_no, ("25", startTimeLong / 1000, 0))
//            }

//            if (huhehaoteFunc) {
//              lastUserStatus.update(phone_no, ("27", startTimeLong / 1000, 0))
//            }
          }

          //          （未离开过的用户不在鄂伦春基站）说明此用户离开了鄂伦春旗
          //          if (elunchunNotLeaveUser.contains(phone_no)
          //            &&
          //            !elunchun.contains(lac_ci)) {
          //            //            向已经离开集合中增加一条新用户的离开信息
          //            elunchunLeaveUser.update(phone_no, "1")
          //            //            删除未离开用户集合
          //            elunchunNotLeaveUser.remove(phone_no)
          //            //            向需要更新的用户表增加一条记录
          //            newElunchunPutMap.update(phone_no, "1")
          //          }

          if (chifengNotLeaveLeaders.contains(phone_no)
            && !local_city.equals("0476")
          ) {
            //            向赤峰离开过的领导的集合中增加一条新用户的离开信息
            chifengLeaveLeaders.update(phone_no, "1")
            //            删除未离开用户集合
            chifengNotLeaveLeaders.remove(phone_no)
            //            向需要更新的赤峰领导表增加一条记录
            newChifengLeaderPutMap.update(phone_no, "1")
          }

          //          如果赤峰已经离开的领导回到了赤峰
          if (chifengLeaveLeaders.contains(phone_no) && local_city.equals("0476")) {
            kafkaProducer.value.send(targetTopic, "2|" + stringLine)
            //            将更新赤峰领导信息重置回0

            newChifengLeaderPutMap.update(phone_no, "0")
          }


        })

        //        将鄂伦春离开的过得用户更新入表中
        //        hunValue.putByKeyColumnList_USER(conn, "TourMasUsualUser", newElunchunPutMap.toList)

        hunValue.putByKeyColumnList_Leader(conn, "TourMasLeaderUser", newChifengLeaderPutMap.toList)


        //        更新新进入计时区域和驻留时长更新区域的用户
        val putResultList: List[(String, (String, Long, Long))] = lastUserStatus
          .filter(!_._2._1.equals("X"))
          .toList
        hunValue.putByKeyColumnList_MAS(conn, "TourMasUser", putResultList)

        //        删除hbase已经离开的用户
        val delResultList = lastUserStatus
          .filter(_._2._1.equals("X"))
          .map(_._1)
          .toList
        hunValue.deleteRows("TourMasUser", delResultList)


        if (conn != null) conn.close()

      }
      )

    })


    ssc.start()
    ssc.awaitTermination()
    ssc.stop()

  }

}


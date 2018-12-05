
import java.util.{Calendar, Properties}

import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.HashPartitioner
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import utils._

/**
  * Created by yaodongzhe on 2017/7/11.
  */
object RoamingAndPartyUserForDHX extends TimeFunc with Serializable {


  var lastTime = Calendar.getInstance().getTime
  val timeFreq: Long = 300000L

  def main(args: Array[String]) {
    val sparkConfig = new SparkConfig
    val conf = sparkConfig.getConf.setAppName("RoamingAndPartyUserForDHX")
    val ssc = new StreamingContext(conf, Seconds(120))
    val hun = new HbaseUtilNew
    val hunBro = ssc.sparkContext.broadcast(hun)


    val topicSet = Array(Set("O_DPI_LTE_S1_MME"), Set("O_DPI_MC_LOCATIONUPDATE_2G"), Set("O_DPI_MC_LOCATIONUPDATE_3G"))

    val kafkaParams = Map[String, String](
      "bootstrap.servers" -> "SS-B02-M20-G03-X3650M5-6:6667,SS-B02-M20-G03-X3650M5-7:6667,SS-B02-M20-G04-X3650M5-1:6667",
      "group.id" -> "RoamingAndPartyUserForDHX",
      "zookeeper.connection.timeout.ms" -> "100000"
    )


    def getPartyUsersFromHbase: List[String] = {
      var listResult = List[String]()
      hun.getAllRows("DHXSelectPhoneList").foreach(r => {
        listResult = Bytes.toString(r.getRow) +: listResult
      })
      listResult
    }


    val lacCiActIdListBro = BroadcastWrapper[List[String]](ssc, getPartyUsersFromHbase)

    def updateBroadcast() {


      //每隔1分钟更新广播变量
      val currTime = toDate
      val diffTime = currTime.getTime - lastTime.getTime
      if (diffTime > timeFreq) {

        lacCiActIdListBro.update(getPartyUsersFromHbase, blocking = true)

        lastTime = toDate
      }
    }


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

      updateBroadcast

      if (lacCiActIdListBro.value.size > 0) {
        rdd.partitionBy(new HashPartitioner(partitions = 100)).foreachPartition(partition => {
          //          kafka-topics.sh --zookeeper zk-01:2181,zk-02:2181,zk-03:2181/kafka --create --replication-factor 1 --partitions 7 --topic ROAMING-AND-PARTY-USER-FOR-DHX
          val targetTopic = "ZNDX-USER-FOR-DHX"
          val area = new AreaList
          val xinganmengArea = new xinganmengAreaList
          val fengzhenArea = new fengzhenAreaList
          val xinganmengWuchakouArea = new XinganmengWuchakouArea
          val ganqimaoduAreaList = new GanqimaoduAreaList
          val xinghexianAreaList = new XinghexianAreaList

          //          val hunValue = hunBro.value
          //          val conn = hunValue.createHbaseConnection
          val aershanLacCiList = area.aershanLacCiList
          val xinBaerhuzuoqi = area.xinBaerhuzuoqi
          val erlianhaote = area.erlianhaote
          val wulagai = area.wulagai
          val alashan = area.alashan
          val elunchun = area.elunchun
          val ejinaqi = area.ejinaqi
          val wulanchabu = area.wulanchabu
          val baotouyidong = area.baotouyidong
          val manzhouli = area.manzhouli
          val siziwangqi = area.siziwangqi
          val keerqinyouyizhongqi = area.keerqinyouyizhongqi
          val xinganmengnongmuyeju1 = xinganmengArea.xinganmengnongmuyeju1
          val xinganmengnongmuyeju2 = xinganmengArea.xinganmengnongmuyeju2
          val xinganmengnongmuyeju3 = xinganmengArea.xinganmengnongmuyeju3
          val wengniute = area.wengniute
          val dengkousanshenggong = area.dengkousanshenggong
          val fengzheng = fengzhenArea.fengzheng
          val tuoxian = area.tuoxian
          val xinganmengWuchakou = xinganmengWuchakouArea.xinganmengWuchakouSet
          val ganqimaodu = ganqimaoduAreaList.ganqimaoduAreaSet
          val wulatehouqi = area.wulatehouqi
          val xinghexian = xinghexianAreaList.xinghexian


          val sortedPartition = partition.toList.sortBy(_._2._1._2)

          //          val distinctPartition = sortedPartition.map(_._1).distinct

          //          val lastUserStatus = hunValue.getResultByKeyList(conn, "TourMasUser", distinctPartition)

          sortedPartition.foreach(kline => {

            val line = kline._2
            val lacCiActIdList = lacCiActIdListBro.value

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


            //            党建人群1
            if (lacCiActIdList.contains(phone_no)) {
              kafkaProducer.value.send(targetTopic, "1|" + stringLine)
            }


            //              兴安盟农牧业局21
            if (roam_type.equals("4")
              && (xinganmengnongmuyeju1.contains(lac_ci)
              || xinganmengnongmuyeju2.contains(lac_ci)
              || xinganmengnongmuyeju3.contains(lac_ci)
              )
            ) {
              kafkaProducer.value.send(targetTopic, "21|" + stringLine)
            }

            //                  乌拉特后旗29
            if (local_city.equals("0478") && wulatehouqi.contains(lac_ci)) {
              kafkaProducer.value.send(targetTopic, "30|" + stringLine)
            }

            //                兴安盟五岔沟28
            if (xinganmengWuchakou.contains(lac_ci)) {
              kafkaProducer.value.send(targetTopic, "28|" + stringLine)
            }
            //            满洲里18
            if (manzhouli.contains(lac_ci)) {
              kafkaProducer.value.send(targetTopic, "18|" + stringLine)
            }
            //              甘其毛都29
            if (ganqimaodu.contains(lac_ci)) {
              kafkaProducer.value.send(targetTopic, "29|" + stringLine)
            }
            //              包头移动17
            else if (baotouyidong.contains(lac_ci)) {
              kafkaProducer.value.send(targetTopic, "17|" + stringLine)
            }
            //              四子王旗19
            else if (siziwangqi.contains(lac_ci)) {
              kafkaProducer.value.send(targetTopic, "19|" + stringLine)
            }
            //            阿尔山5
            //            else if (local_city.equals("0482") && aershanLacCiList.contains(lac_ci)) {
            else if (local_city.equals("0482")) {
              kafkaProducer.value.send(targetTopic, "5|" + stringLine)
            }
            //              乌兰察布，兴和县31
            else if (local_city.equals("0474") && xinghexian.contains(lac_ci)) {
              kafkaProducer.value.send(targetTopic, "31|" + stringLine)
            }
            else {
              if (!roam_type.equals("4") && !roam_type.equals("")) {

                //                呼和浩特27
                if (local_city.equals("0471") && !owner_city.equals("0471")) {
                  kafkaProducer.value.send(targetTopic, "27|" + stringLine)
                }
                //                翁牛特旗22
                if (local_city.equals("0476") && wengniute.contains(lac_ci)) {
                  kafkaProducer.value.send(targetTopic, "22|" + stringLine)
                }
                //                乌兰察布，丰镇26
                if (local_city.equals("0474") && fengzheng.contains(lac_ci)) {
                  kafkaProducer.value.send(targetTopic, "26|" + stringLine)
                }
                //                磴口三盛公景区24
                if (local_city.equals("0478") && dengkousanshenggong.contains(lac_ci)) {
                  kafkaProducer.value.send(targetTopic, "24|" + stringLine)
                }

                //            漫入赤峰人群(所在地市，漫游类型)id:12
                else if (local_city.equals("0476")) {
                  kafkaProducer.value.send(targetTopic, "12|" + stringLine)
                }
                //            新巴尔虎左旗6
                else if (local_city.equals("0470") && xinBaerhuzuoqi.contains(lac_ci)) {
                  kafkaProducer.value.send(targetTopic, "6|" + stringLine)
                }
                //            二连浩特3
                else if (local_city.equals("0479") && erlianhaote.contains(lac_ci)) {
                  kafkaProducer.value.send(targetTopic, "3|" + stringLine)
                }
                //            锡林郭勒乌拉盖4
                else if (local_city.equals("0479") && wulagai.contains(lac_ci)) {
                  kafkaProducer.value.send(targetTopic, "4|" + stringLine)
                }
                //              阿拉善右旗8
                else if (local_city.equals("0483") && alashan.contains(lac_ci)) {
                  kafkaProducer.value.send(targetTopic, "8|" + stringLine)
                }
                //              乌兰察布10
                else if (local_city.equals("0474") && wulanchabu.contains(lac_ci)) {
                  kafkaProducer.value.send(targetTopic, "10|" + stringLine)
                }
                //              阿拉善额济纳旗13
                else if (local_city.equals("0483") && ejinaqi.contains(lac_ci)) {
                  kafkaProducer.value.send(targetTopic, "13|" + stringLine)
                }
                //                  科尔沁右翼中旗20
                else if (local_city.equals("0482") && keerqinyouyizhongqi.contains(lac_ci)) {
                  kafkaProducer.value.send(targetTopic, "20|" + stringLine)
                }
                //                  呼和浩特，托县25
                else if (local_city.equals("0471") && tuoxian.contains(lac_ci)) {
                  kafkaProducer.value.send(targetTopic, "25|" + stringLine)
                }


                //              if(roam_type.equals("2")){
                //                kafkaProducer.value.send(targetTopic, "16|" + stringLine)
                //              }

              }


            }


          })

        }
        )
      }
    })


    ssc.start()
    ssc.awaitTermination()
    ssc.stop()

  }

}



import java.util.{Calendar, Properties}

import area._
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
  var gonganLastTime = Calendar.getInstance().getTime
  val timeFreq: Long = 300000L
  val gonganTimeFreq: Long = timeFreq

  def main(args: Array[String]) {
    val sparkConfig = new SparkConfig
    val conf = sparkConfig.getConf.setAppName("RoamingAndPartyUserForDHX")
    val ssc = new StreamingContext(conf, Seconds(20))
    val hun = new HbaseUtilNew


    val topicSet = Array(Set("O_DPI_LTE_S1_MME"), Set("O_DPI_MC_LOCATIONUPDATE_2G"), Set("O_DPI_MC_LOCATIONUPDATE_3G"))

    val kafkaParams = Map[String, String](
      "bootstrap.servers" -> "SS-B02-M20-G03-X3650M5-6:6667,SS-B02-M20-G03-X3650M5-7:6667,SS-B02-M20-G04-X3650M5-1:6667",
      "group.id" -> "RoamingAndPartyUserForDHX",
      "zookeeper.connection.timeout.ms" -> "100000"
    )


    def getPartyUsersFromHbase: Set[String] = {
      var ResultSet = Set[String]()
      hun.getAllRows("DHXSelectPhoneList").foreach(r => {
        ResultSet = ResultSet.+(Bytes.toString(r.getRow))
      })
      ResultSet
    }

    def getGonganCustFromHdfs: Set[String] = {
      val gonganPath = "hdfs://nmdsj133nds/user/caodongwei/pdata_floatingnet_sc_phonelist/*"
      ssc.sparkContext.textFile(gonganPath).collect().toSet
    }


    val lacCiActIdSetBro = BroadcastWrapper[Set[String]](ssc, getPartyUsersFromHbase)
    val gonganCustSetBro = BroadcastWrapper[Set[String]](ssc, getGonganCustFromHdfs)

    def updateBroadcast() {


      //每隔5分钟更新广播变量
      val currTime = toDate
      val diffTime = currTime.getTime - lastTime.getTime
      if (diffTime > timeFreq) {

        lacCiActIdSetBro.update(getPartyUsersFromHbase, blocking = true)
        lastTime = toDate
      }
    }

    def updateGonganBroadcast() {

      //每隔3小时更新广播变量
      val currTime = toDate
      val diffTime = currTime.getTime - gonganLastTime.getTime
      if (diffTime > gonganTimeFreq) {

        gonganCustSetBro.update(getGonganCustFromHdfs, blocking = true)
        gonganLastTime = toDate
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
      updateGonganBroadcast

      rdd.partitionBy(new HashPartitioner(partitions = 100)).foreachPartition(partition => {
        //          kafka-topics.sh --zookeeper zk-01:2181,zk-02:2181,zk-03:2181/kafka --create --replication-factor 1 --partitions 7 --topic ROAMING-AND-PARTY-USER-FOR-DHX
        val gonganCustSet = gonganCustSetBro.value
        val lacCiActIdSet = lacCiActIdSetBro.value

        val targetTopic = "ZNDX-USER-FOR-DHX"
        val area = new AreaList
        val xinganmengArea = new xinganmengAreaList
        val fengzhenArea = new fengzhenAreaList
        val xinganmengWuchakouAreaList = new XinganmengWuchakouAreaList
        val ganqimaoduAreaList = new GanqimaoduAreaList
        val xinghexianAreaList = new XinghexianAreaList
        val chenbaerhuqiAreaList = new ChenbaerhuqiAreaList
        val eerduosiAreaList = new EerduosiAreaList
        val elunchunJijianjianchaAreaList = new ElunchunJijianjianchaAreaList
        val elunchungonganjuAreaList = new ElunchungonganjuAreaList
        val erlianhaotexuanchuanbuAreaList = new ErlianhaotexuanchuanbuAreaList
        val yijinhuoluoqiAreaList = new YijinhuoluoqiAreaList
        val elunchunqiweixuanchuanbuAreaList = new ElunchunqiweixuanchuanbuAreaList
        val genheAlongshanAreaList = new GenheAlongshanAreaList
        val tuoketuoxianAreaList = new TuoketuoxianAreaList
        val genhelinyejuAreaList = new GenhelinyejuAreaList
        val chuoyuanlinyejuAreaList = new ChuoyuanlinyejuAreaList
        val bailanglinyejuAreaList = new BailanglinyejuAreaList
        val dalateqiAreaList = new DalateqiAreaList
        val mianduheAreaList = new MianduheAreaList
        val wuerqiAreaList = new WuerqiAreaList
        val baotounongshanghangAreaList = new BaotounongshanghangAreaList
        val bamengbianjingzhiduAreaList = new BamengbianjingzhiduAreaList
        val tumujibaohuquAreaList = new TumujibaohuquAreaList
        val wulagaiGuanliquAreaList = new WulagaiGuanliquAreaList
        val chuoerlinyejuAreaList = new ChuoerlinyejuAreaList
        val budui32107AreaList = new Budui32107AreaList
        val jinhesenlingongyeAreaList = new JinhesenlingongyeAreaList
        val daxinganlinglinguanjuAreaList = new DaxinganlinglinguanjuAreaList
        val shangduxuanchuanbuAreaList = new ShangduxuanchuanbuAreaList
        val budui66113AreaList = new Budui66113AreaList
        val yituliheAreaList = new YituliheAreaList
        val eqianqiAreaList = new EqianqiAreaList
        val ganhelinyeAreaList = new GanhelinyeAreaList


        val aershanLacCiList = area.aershanLacCiList
        val xinBaerhuzuoqi = area.xinBaerhuzuoqi
        val erlianhaote = area.erlianhaote
        //        val wulagai = area.wulagai
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
        val dengkousanshenggong = area.dengkousanshenggong
        val fengzheng = fengzhenArea.fengzheng
        val tuoketuoxian = tuoketuoxianAreaList.tuoketuoxian
        val xinganmengWuchakou = xinganmengWuchakouAreaList.xinganmengWuchakouSet
        val ganqimaodu = ganqimaoduAreaList.ganqimaoduAreaSet
        val wulatehouqi = area.wulatehouqi
        val xinghexian = xinghexianAreaList.xinghexian
        val chenbaerhuqi = chenbaerhuqiAreaList.chenbaerhuqi
        val eerduosi = eerduosiAreaList.eerduosi
        val eerduosiLeaders = eerduosiAreaList.eerduosiLeaders
        val elunchunJijianjiancha = elunchunJijianjianchaAreaList.elunchunJijianjiancha
        val elunchungonganju = elunchungonganjuAreaList.elunchungonganju
        val erlianhaotexuanchuanbu = erlianhaotexuanchuanbuAreaList.erlianhaotexuanchuanbu
        val yijinhuoluoqi = yijinhuoluoqiAreaList.yijinhuoluoqi
        val elunchunqiweixuanchuanbu = elunchunqiweixuanchuanbuAreaList.elunchunqiweixuanchuanbu
        val genheAlongshan = genheAlongshanAreaList.genheAlongshan
        val bailanglinyeju = bailanglinyejuAreaList.bailanglinyeju
        val chuoyuanlinyeju = chuoyuanlinyejuAreaList.chuoyuanlinyeju
        val genhelinyeju = genhelinyejuAreaList.genhelinyeju
        val dalateqi = dalateqiAreaList.dalateqi
        val mianduhe = mianduheAreaList.mianduhe
        val wuerqi = wuerqiAreaList.wuerqi
        val baotounongshanghang = baotounongshanghangAreaList.baotounongshanghang
        val bamengbianjing = bamengbianjingzhiduAreaList.Bamengbianjing
        val tumujibaohuqu = tumujibaohuquAreaList.tumujibaohuqu
        val wulagaiGuanliqu = wulagaiGuanliquAreaList.wulagaiguanliqu
        val chuoerlinyeju = chuoerlinyejuAreaList.chuoerlinyeju
        val budui32107 = budui32107AreaList.Budui32107
        val jinhesenlingongye = jinhesenlingongyeAreaList.jinhesenlingongye
        val daxinganlinglinguanju = daxinganlinglinguanjuAreaList.daxinganlinglinguanju
        val shangduxuanchuanbu = shangduxuanchuanbuAreaList.shangduxuanchuanbu
        val budui66113 = budui66113AreaList.budui66113
        val yitulihe = yituliheAreaList.yitulihe
        val eqianqiChaao = eqianqiAreaList.eqianqiChaao
        val eqianqiAoang = eqianqiAreaList.eqianqiAoang
        val eqianqiAoyin = eqianqiAreaList.eqianqiAoyin
        val eqianqiAodong = eqianqiAreaList.eqianqiAodong
        val eqianqiSanyan = eqianqiAreaList.eqianqiSanyan
        val eqianqiS216 = eqianqiAreaList.eqianqiS216
        val eqianqiTongshi = eqianqiAreaList.eqianqiTongshi
        val eqianqiX632 = eqianqiAreaList.eqianqiX632
        val ganhelinye = ganhelinyeAreaList.ganhelinye

        partition
          .toList
          .sortBy(_._2._1._2)
          .foreach(kline => {

            val line = kline._2

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


            def send(eventType: Int): Unit = {
              kafkaProducer.value.send(targetTopic, eventType.toString + "|" + stringLine)
            }
            //            党建人群1
            if (lacCiActIdSet.contains(phone_no)) send(1)
            //            公安人群32
            if (gonganCustSet.contains(phone_no)) send(32)
            //              兴安盟农牧业局21
            //            if (roam_type.equals("4") && (xinganmengnongmuyeju1.contains(lac_ci)
            //              || xinganmengnongmuyeju2.contains(lac_ci)
            //              || xinganmengnongmuyeju3.contains(lac_ci)
            //              )
            //            ) send(21)
            //                  乌拉特后旗30
            //            if (local_city.equals("0478") && wulatehouqi.contains(lac_ci)) send(30)
            //            阿尔山5
            if (local_city.equals("0482")) send(5)
            //              鄂伦春旗委宣传部11
            if (elunchunqiweixuanchuanbu.contains(lac_ci)) send(11)
            //              包头移动17
            if (baotouyidong.contains(lac_ci)) send(17)
            //                兴安盟五岔沟28
            if (xinganmengWuchakou.contains(lac_ci)) send(28)
            //            满洲里18
            //            if (manzhouli.contains(lac_ci)) send(18)
            //              甘其毛都29
            if (ganqimaodu.contains(lac_ci)) send(29)
            //              鄂伦春纪检监察34
            if (elunchunJijianjiancha.contains(lac_ci)) send(34)
            //              鄂伦春公安局35
            if (elunchungonganju.contains(lac_ci)) send(35)
            //            二连浩特宣传部36
            if (erlianhaotexuanchuanbu.contains(lac_ci)) send(36)
            //            根河阿龙山39
            if (genheAlongshan.contains(lac_ci)) send(39)
            //            呼伦贝尔，绰源林业局40
            if (chuoyuanlinyeju.contains(lac_ci)) send(40)
            //            呼伦贝尔，根河林业局41
            if (genhelinyeju.contains(lac_ci)) send(41)
            //            兴安盟，白狼林业局42
            if (bailanglinyeju.contains(lac_ci)) send(42)
            //            呼伦贝尔，牙克石，免渡河44
            if (mianduhe.contains(lac_ci)) send(44)
            //            呼伦贝尔，牙克石，乌尔旗45
            if (wuerqi.contains(lac_ci)) send(45)
            //            巴彦淖尔边境管理支队48
            if (bamengbianjing.contains(lac_ci)) send(48)
            //            兴安盟，扎赉特旗，图牧吉保护区49
            if (tumujibaohuqu.contains(lac_ci)) send(49)
            //            锡林郭勒盟，乌拉盖54
            if (wulagaiGuanliqu.contains(lac_ci)) send(54)
            //            呼伦贝尔，绰尔林业局56
            if (chuoerlinyeju.contains(lac_ci)) send(56)
            //            32107部队57
            if (budui32107.contains(lac_ci)) send(57)
            //            金河森林工业58
            if (jinhesenlingongye.contains(lac_ci)) send(58)
            //            大兴安岭林管局59
            if (daxinganlinglinguanju.contains(lac_ci)) send(59)
            //            66113部队61
            if (budui66113.contains(lac_ci)) send(61)
            //            伊图里河62
            if (yitulihe.contains(lac_ci)) send(62)

            //            鄂前旗察敖63
            if (eqianqiChaao.contains(lac_ci)) send(63)
            //            鄂前旗敖昂64
            if (eqianqiAoang.contains(lac_ci)) send(64)
            //            鄂前旗敖银65
            if (eqianqiAoyin.contains(lac_ci)) send(65)
            //            鄂前旗敖东66
            if (eqianqiAodong.contains(lac_ci)) send(66)
            //            鄂前旗三盐67
            if (eqianqiSanyan.contains(lac_ci)) send(67)
            //            鄂前旗S216 68
            if (eqianqiS216.contains(lac_ci)) send(68)
            //            鄂前旗通史 69
            if (eqianqiTongshi.contains(lac_ci)) send(69)
            //            鄂前旗X632 70
            if (eqianqiX632.contains(lac_ci)) send(70)
            //            甘河林业局71
            if (ganhelinye.contains(lac_ci)) send(71)

            //              四子王旗19
            //            if (siziwangqi.contains(lac_ci)) send(19)

            //            else if (local_city.equals("0482") && aershanLacCiList.contains(lac_ci)) {


            //              漫入人群
            if (!roam_type.equals("4") && !roam_type.equals("")) {

              //                乌兰察布，丰镇26
              if (local_city.equals("0474") && fengzheng.contains(lac_ci)) send(26)
              //                呼和浩特27
              if (local_city.equals("0471") && !owner_city.equals("0471")) send(27)
              //                翁牛特旗22
              //              if (local_city.equals("0476") && wengniute.contains(lac_ci)) send(22)
              //                鄂尔多斯，伊金霍洛旗37
              if (local_city.equals("0477") && yijinhuoluoqi.contains(lac_ci)) send(37)
              //              鄂尔多斯，达拉特旗43（内蒙古圣景文化旅游发展有限责任公司）
              if (local_city.equals("0477") && dalateqi.contains(lac_ci)) send(43)
              //                兴安盟，旅游局50
              if (local_city.equals("0482")) send(50)
              //              内蒙古文旅厅51
              if (roam_type.equals("2")) send(51)
              //              巴彦淖尔，经济和信息化委员会52
              if (local_city.equals("0478")) send(52)
              //              鄂尔多斯，达拉特旗53（鄂尔多斯市恩格贝沙漠生态旅游文化有限责任公司）
              if (local_city.equals("0477") && dalateqi.contains(lac_ci)) send(53)

              //                磴口三盛公景区24
              //              if (local_city.equals("0478") && dengkousanshenggong.contains(lac_ci)) send(24)
              //            漫入赤峰人群(所在地市，漫游类型)id:12
              else if (local_city.equals("0476")) send(12)
              //            呼伦贝尔，新巴尔虎左旗6
              else if (local_city.equals("0470") && xinBaerhuzuoqi.contains(lac_ci)) send(6)
              //            呼伦贝尔，陈巴尔虎旗31
              else if (local_city.equals("0470") && chenbaerhuqi.contains(lac_ci)) send(31)
              //            二连浩特3
              //              else if (local_city.equals("0479") && erlianhaote.contains(lac_ci)) send(3)
              //            锡林郭勒乌拉盖4
              //              else if (local_city.equals("0479") && wulagai.contains(lac_ci)) send(4)
              //              阿拉善右旗8
              //              else if (local_city.equals("0483") && alashan.contains(lac_ci)) send(8)
              //              乌兰察布10
              else if (local_city.equals("0474") && wulanchabu.contains(lac_ci)) send(10)
              //              阿拉善额济纳旗13
              else if (local_city.equals("0483") && ejinaqi.contains(lac_ci)) send(13)
              //                  科尔沁右翼中旗20
              //              else if (local_city.equals("0482") && keerqinyouyizhongqi.contains(lac_ci)) send(20)
              //                  呼和浩特，托县25
              //              else if (local_city.equals("0471") && tuoxian.contains(lac_ci)) send(25)
              //                鄂尔多斯7
              else if (local_city.equals("0477") && eerduosi.contains(lac_ci)) send(7)
            }
            //            非漫入人群，本地用户
            else {
              //            呼和浩特，托克托县25
              if (local_city.equals("0471")
                && tuoketuoxian.contains(lac_ci)
                && owner_city.equals("0471")
                && !roam_type.equals("")
              ) send(25)

              //              包头，农商行46
              if (!roam_type.equals("")
                && baotounongshanghang.contains(lac_ci)
              ) send(46)

              //              乌兰察布，商都宣传部60
              if (!roam_type.equals("")
                && shangduxuanchuanbu.contains(lac_ci)
              ) send(60)

            }
          })
      }
      )
    })


    ssc.start()
    ssc.awaitTermination()
    ssc.stop()

  }

}


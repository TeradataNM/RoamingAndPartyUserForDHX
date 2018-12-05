package utils

import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.{FilterList, SingleColumnValueFilter}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration, TableName}

import scala.collection.JavaConversions._
import scala.collection.{Map, mutable}
import scala.collection.mutable.ListBuffer

class HbaseUtilNew extends Serializable {


  var conf: Configuration = null;

  def init() {
    this.synchronized {
      if (conf != null) {
        return
      }

      conf = HBaseConfiguration.create()
      conf.set("hadoop.security.bdoc.access.id", "d690843cba5facbce853")
      conf.set("hadoop.security.bdoc.access.key", "2e6212edd46b91aa3055baa965b01f6f8cf9f482")
      //  conf.addResource("hbase-site.xml.bc132")
      //  conf.addResource("core-site.xml")
      conf.set("fs.defaultFS", "hdfs://nmdsj133nds")
      conf.set("hbase.zookeeper.quorum", "ss-b02-m20-d11-r730-1,ss-b02-m20-d12-r730-1,ss-b02-m20-d12-r730-2,ss-b02-m20-d13-r730-1,ss-b02-m20-d13-r730-2")
      conf.set("hbase.zookeeper.property.clientPort", "2181")
      conf.set("zookeeper.znode.parent", "/IOP_TD")

      //      pause时长，在hbase发生get或其他操作fail掉的时候进行pause的时间长度
      //      所以如果重试10次,hbase.client.pause=50ms，则每次重试等待时间为{50，100，150，250，500，1000，2000，5000，5000，5000}。
      conf.set("hbase.client.pause", "1000")
      //      "hbase.client.retries.number"默认35次
      //      conf.set("hbase.client.retries.number", "100")
      //      Hbase client发起远程调用时的超时时限，使用ping来确认连接，但是最终会抛出一个TimeoutException，默认值是60000；
      //      conf.set("hbase.rpc.timeout", "12000")
      conf.set("hbase.client.operation.timeout", "60000")
      conf.set("hbase.client.scanner.timeout.period", "10000")
      conf.set("hbase.client.write.buffer", "6291456")
      conf.set("hbase.zookeeper.property.maxClientCnxns", "1000")
      conf.set("hbase.regionserver.handler.count", "300")

    }
  }

  def createHbaseConnection: Connection = {
    this.init()

    val conn = ConnectionFactory.createConnection(conf)
    conn
  }

  //  /**
  //    * 获取HBase连接
  //    *
  //    * @return conn: Connection
  //    */
  //  def getHbaseConn: Connection = {
  //
  //
  //    var poolConn: Connection = null
  //    var loopBool = true
  //    for (times <- 0 to 3 if loopBool) {
  //      try {
  //        poolConn = pool.getConnection
  //        loopBool = false
  //      } catch {
  //        case e: Exception =>
  //          Thread.sleep(2000L)
  //      }
  //    }
  //    poolConn
  //  }

  /**
    * 根据表的RowKey、列族、列，获取结果，放到Result中
    *
    * @param conn      Hbase连接
    * @param tableName 表名称
    * @param rowKey    RowKey
    * @param family    列族名
    * @param qualifier 列名
    * @return
    */
  def getValuesByKeyColumn(conn: Connection, tableName: String, rowKey: String, family: String, qualifier: String): Result = {
    var result: Result = null
    val table: Table = conn.getTable(TableName.valueOf(tableName))
    try {
      val g = new Get(Bytes.toBytes(rowKey))
      g.addColumn(family.getBytes(), qualifier.getBytes())
      result = table.get(g)
    } finally {
      if (table != null) table.close()
    }
    result
  }

  def getValuesByKeyColumn(conn: Connection, tableName: String, rowKey: String, family: String): Result = {
    var result: Result = null
    var table: Table = null
    try {
      table = conn.getTable(TableName.valueOf(tableName))
      val g = new Get(Bytes.toBytes(rowKey))
      g.addFamily(family.getBytes())
      result = table.get(g)
    } catch {
      case en: NullPointerException => println("---------NullPointerException---------:" + tableName + " " + rowKey)
    } finally {
      if (table != null) table.close()
    }
    result
  }


  /**
    * 根据表名称、RowKey、列族名、列名、值，写入Hbase
    *
    * @param conn      HBase连接
    * @param tableName 表名
    * @param rowKey    RowKey
    * @param family    列族名
    * @param qualifier 列名
    * @param value     Value
    */
  def putByKeyColumn(conn: Connection, tableName: String, rowKey: String, family: String, qualifier: String, value: String): Unit = {
    val table = conn.getTable(TableName.valueOf(tableName))
    try {
      //准备插入一条 key 为 rowKey 的数据
      val p = new Put(rowKey.getBytes).setWriteToWAL(false)
      //为put操作指定 column 和 value （以前的 put.add 方法被弃用了）
      p.addColumn(family.getBytes, qualifier.getBytes, value.getBytes)
      //提交
      table.put(p)
    } finally {
      if (table != null) table.close()
    }
  }

  def putByKeyColumnList_MAS(conn: Connection, tableName: String, resultList: List[(String, (String, Long, Long))]): Unit = {
    val table = conn.getTable(TableName.valueOf(tableName))
    try {
      val muList = new ListBuffer[Put]

      resultList.foreach(res => {
        val rphone_no = res._1
        val phone_no = rphone_no.substring(rphone_no.length - 2, rphone_no.length) + rphone_no
        val eventType = res._2._1
        val startTime = res._2._2
        val duration = res._2._3

        muList.add(
          new Put(phone_no.getBytes)
            .addColumn("0".getBytes(), "eventType".getBytes(), eventType.getBytes())
            .addColumn("0".getBytes(), "startTime".getBytes(), startTime.toString.getBytes())
            .addColumn("0".getBytes(), "duration".getBytes(), duration.toString.getBytes())
        )
      })

      table.put(muList)
    } finally {
      if (table != null) table.close()
    }
  }

  def putByKeyColumnList_USER(conn: Connection, tableName: String, resultList: List[(String, String)]): Unit = {
    val table = conn.getTable(TableName.valueOf(tableName))
    try {
      val muList = new ListBuffer[Put]

      resultList.foreach(res => {
        val rphone_no = res._1
        val phone_no = rphone_no.substring(rphone_no.length - 2, rphone_no.length) + rphone_no
        val flag = res._2

        muList.add(
          new Put(phone_no.getBytes)
            .addColumn("0".getBytes(), "flag".getBytes(), flag.getBytes())
        )
      })

      table.put(muList)
    } finally {
      if (table != null) table.close()
    }
  }

  def putByKeyColumnList_Leader(conn: Connection, tableName: String, resultList: List[(String, String)]): Unit = {
    val table = conn.getTable(TableName.valueOf(tableName))
    try {
      val muList = new ListBuffer[Put]

      resultList.foreach(res => {
        val phone_no = res._1
        val flag = res._2

        muList.add(
          new Put(phone_no.getBytes)
            .addColumn("0".getBytes(), "flag".getBytes(), flag.getBytes())
        )
      })

      table.put(muList)
    } finally {
      if (table != null) table.close()
    }
  }


  def putByKeyColumn(conn: Connection, tableName: String, rowKey: String, family: String, qualifierValueList: Iterator[(String, String)]): Unit = {
    val table = conn.getTable(TableName.valueOf(tableName))
    try {
      //准备插入一条 key 为 rowKey 的数据
      val p = new Put(rowKey.getBytes).setWriteToWAL(false)
      //为put操作指定 column 和 value （以前的 put.add 方法被弃用了）
      while (qualifierValueList.hasNext) {
        val qualifierValue = qualifierValueList.next()
        val qualifier = qualifierValue._1
        val value = qualifierValue._2
        p.addColumn(family.getBytes, qualifier.getBytes, value.getBytes)
      }
      //提交
      table.put(p)
    } finally {
      if (table != null) table.close()
    }
  }

  def getAllRows(tableName: String, family: String, qualifier: String): Map[String, String] = {
    this.init()
    val table: HTable = new HTable(conf, tableName)
    val results: ResultScanner = table.getScanner(new Scan())
    val it: util.Iterator[Result] = results.iterator()
    val lstBuffer = ListBuffer[(String, String)]()
    while (it.hasNext) {
      val next: Result = it.next()
      lstBuffer.+=((new String(next.getRow), new String(next.getValue(family.getBytes, qualifier.getBytes))))
    }
    lstBuffer.toMap
  }


  def getAllRows(tableName: String): List[Result] = {
    this.init()
    val table: HTable = new HTable(conf, tableName)
    val rs: ResultScanner = table.getScanner(new Scan())
    val rsList: List[Result] = rs.toList

    rs.close()
    table.close()
    rsList

  }

  def getResultByKeyList_MAS(conn: Connection, tableName: String, keyList: List[String]): mutable.HashMap[String, (String, Long, Long)] = {
    val table = conn.getTable(TableName.valueOf(tableName))
    val results: mutable.HashMap[String, (String, Long, Long)] = new scala.collection.mutable.HashMap
    try {
      val muList = new ListBuffer[Get]
      keyList.foreach(rphone_no => {
        val phone_no = rphone_no.substring(rphone_no.length - 2, rphone_no.length) + rphone_no
        muList.add(
          new Get(phone_no.getBytes).addFamily("0".getBytes())
        )
      })

      val getList = table.get(muList)
      getList.foreach(result => {
        if (!result.isEmpty) {
          val prePhoneNo = Bytes.toString(result.getRow)
          val phoneNo = prePhoneNo.substring(2)
          val eventType = Bytes.toString(result.getValue("0".getBytes(), "eventType".getBytes()))
          val startTime = Bytes.toString(result.getValue("0".getBytes(), "startTime".getBytes())).toLong
          val duration = Bytes.toString(result.getValue("0".getBytes(), "duration".getBytes())).toLong
          results.update(phoneNo, (eventType, startTime, duration))
        }
      })


    } finally {
      if (table != null) table.close()
    }

    results
  }


  def getResultByKeyList_USER(conn: Connection, tableName: String, keyList: List[String]): mutable.HashMap[String, String] = {
    val table = conn.getTable(TableName.valueOf(tableName))
    val results: mutable.HashMap[String, String] = new scala.collection.mutable.HashMap
    val area = new AreaList
    try {
      val muList = new ListBuffer[Get]
      keyList.foreach(rphone_no => {
        val phone_no = rphone_no.substring(rphone_no.length - 2, rphone_no.length) + rphone_no
        muList.add(
          new Get(phone_no.getBytes).addFamily("0".getBytes())
        )
      })

      table.get(muList).foreach(result => {
        if (!result.isEmpty) {
          val prePhoneNo = Bytes.toString(result.getRow)
          val phoneNo = prePhoneNo.substring(2)
          val lac_cell = Bytes.toString(result.getValue("0".getBytes(), "0".getBytes()))
          //          如果用户的常驻基站在鄂伦春旗内
          if (area.elunchun.contains(lac_cell)) {
            try {
              val flag = Bytes.toString(result.getValue("0".getBytes(), "flag".getBytes()))
              if (flag != null) results.update(phoneNo, flag)
              else results.update(phoneNo, "0")
            } catch {
              case e => None
            }
          }
        }
      })


    } finally {
      if (table != null) table.close()
    }

    results
  }

  def getResultByKeyList_Leader(conn: Connection, tableName: String, family: String, qualiafier: String, keyList: List[String]): mutable.HashMap[String, String] = {
    val table = conn.getTable(TableName.valueOf(tableName))
    val results: mutable.HashMap[String, String] = new scala.collection.mutable.HashMap
    val area = new AreaList
    try {
      val muList = new ListBuffer[Get]
      keyList.foreach(phone_no => {
        muList.add(
          new Get(phone_no.getBytes).addFamily("0".getBytes())
        )
      })

      table.get(muList).foreach(result => {
        if (!result.isEmpty) {
          val phoneNo = Bytes.toString(result.getRow)
          try {
            //              只查找离开过得用户
            val flag = Bytes.toString(result.getValue("0".getBytes(), "flag".getBytes()))
            if (flag != null) results.update(phoneNo, flag)
            else results.update(phoneNo, "0")
          } catch {
            case e => None
          }

        }
      })


    } finally {
      if (table != null) table.close()
    }

    results
  }

  def getResultByFilter(tableName: String): ListBuffer[String] = {
    this.init()
    val area = new AreaList
    val elunchun = area.elunchun
    val filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE)
    val resultSet = new ListBuffer[String]()
    elunchun.foreach(lac_cell => {
      val filter = new SingleColumnValueFilter("0".getBytes(), "0".getBytes(), CompareOp.EQUAL, lac_cell.getBytes)
      filterList.addFilter(filter)
    })

    val table: HTable = new HTable(conf, tableName)
    val results: ResultScanner = table.getScanner((new Scan()).setFilter(filterList))
    results.foreach(r => {
      val prePhoneNo = Bytes.toString(r.getRow)
      val phoneNo = prePhoneNo.substring(2)
      resultSet.add(phoneNo)
    })
    resultSet
  }

  def deleteRows(tablename: String, rowkeys: List[String]): Unit = {
    this.init()
    val table = new HTable(conf, tablename)
    val muList = new ListBuffer[Delete]
    rowkeys.foreach((rphone_no: String) => {
      val phone_no = rphone_no.substring(rphone_no.length - 2, rphone_no.length) + rphone_no
      muList.add(new Delete(phone_no.getBytes()))
    }
    )

    table.delete(muList)
    table.close();
  }

}






package utils

import java.io.{ObjectInputStream, ObjectOutputStream}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.StreamingContext

import scala.reflect.ClassTag

/**
  * Created by lyndon on 2017/5/5.
  */
case class BroadcastWrapper[T: ClassTag]
(@transient private val ssc: StreamingContext,
 @transient private val _v: T) {

  @transient private var v: Broadcast[T] = ssc.sparkContext.broadcast(_v)

  def update(newValue: T, blocking: Boolean = false): Unit = {
    // 删除RDD是否需要锁定
    v.unpersist(blocking)
    v = ssc.sparkContext.broadcast(newValue)
  }

  def value: T = v.value



  private def writeObject(out: ObjectOutputStream): Unit = {
    out.writeObject(v)
  }

  private def readObject(in: ObjectInputStream): Unit = {
    v = in.readObject.asInstanceOf[Broadcast[T]]
  }
}

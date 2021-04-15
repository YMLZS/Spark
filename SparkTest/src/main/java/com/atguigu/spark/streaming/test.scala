package com.atguigu.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Duration, StreamingContext}

/**
  * @author yml
  * 2021-03-22-19:43
  */
object test {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
    val sc = new StreamingContext(conf,Duration(3000))
    //val ds: ReceiverInputDStream[String] = sc.socketTextStream("localhost",9999)
    val ds: ReceiverInputDStream[String] = sc.receiverStream(new MyReceiver)

    ds.print()

    sc.start()
    sc.awaitTermination()

  }

  class MyReceiver extends Receiver[String](StorageLevel.MEMORY_ONLY){
    private var flag : Boolean = true

    override def onStart(): Unit = {
      while(flag){
        var name : String = "liyichen - laosepi" + System.currentTimeMillis()
        store(name)
        Thread.sleep(1000)
      }
    }

    override def onStop(): Unit = {
      flag = false
    }
  }

}

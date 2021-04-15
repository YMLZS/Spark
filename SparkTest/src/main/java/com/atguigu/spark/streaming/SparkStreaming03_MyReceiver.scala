package com.atguigu.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Duration, StreamingContext}

import scala.collection.mutable

/**
  * @author yml
  * 2021-03-20-16:27
  */
object SparkStreaming03_MyReceiver {

  def main(args: Array[String]): Unit = {

    //TODO 从自定义采集器中创建DStream
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("SparkStreaming")
    val ssc = new StreamingContext(conf,Duration(3000))

    val ds: ReceiverInputDStream[String] = ssc.receiverStream(new MyReceiver)
    ds.print()

    ssc.start()
    ssc.awaitTermination()

  }

  class MyReceiver extends Receiver[String](StorageLevel.MEMORY_ONLY){

    private var flag : Boolean = true

    //采集数据（源源不断）
    override def onStart(): Unit = {

      while(flag){
        //生成模拟数据
        val name = "zhangsan - " + System.currentTimeMillis()
        //将数据交给采集器
        store(name)

        Thread.sleep(1000)
      }

    }

    //释放资源
    override def onStop(): Unit = {
      flag = false
    }

  }

}

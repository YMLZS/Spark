package com.atguigu.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

/**
  * @author yml
  * 2021-03-20-16:27
  */
object SparkStreaming09_Window {

  def main(args: Array[String]): Unit = {

    //TODO 滑动窗口
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("SparkStreaming")
    val ssc = new StreamingContext(conf,Duration(3000))

    val sds: ReceiverInputDStream[String] = ssc.socketTextStream("localhost",9999)
    val words: DStream[String] = sds.flatMap(_.split(" "))
    val a: DStream[(String, Int)] = words.map((_, 1))

    //窗口的大小必须为采集周期的整数倍
    //窗口滑动的大小也必须是采集周期的整数倍
    val b: DStream[(String, Int)] = a.window(Seconds(6),Seconds(3))

    val count: DStream[(String, Int)] = b.reduceByKey(_+_)
    count.print()


    ssc.start()
    ssc.awaitTermination()

  }

}

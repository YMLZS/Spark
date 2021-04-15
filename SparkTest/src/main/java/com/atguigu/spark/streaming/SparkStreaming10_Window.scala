package com.atguigu.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

/**
  * @author yml
  * 2021-03-20-16:27
  */
object SparkStreaming10_Window {

  def main(args: Array[String]): Unit = {

    //TODO 滑动窗口
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("SparkStreaming")
    val ssc = new StreamingContext(conf,Duration(3000))
    //
    ssc.checkpoint("cp")

    val sds: ReceiverInputDStream[String] = ssc.socketTextStream("localhost",9999)
    val words: DStream[String] = sds.flatMap(_.split(" "))
    val a: DStream[(String, Int)] = words.map((_, 1))

    //当窗口大小 > 滑动大小时，会有重复数据现象，为了避免重复计算，可以使用reduceByKeyAndWindow
    val count: DStream[(String, Int)] = a.reduceByKeyAndWindow(
      (x, y) => {
        x + y
      },
      (x, y) => {
        x - y
      },
      Seconds(9),
      Seconds(3)
    )

    count.print()

    ssc.start()
    ssc.awaitTermination()

  }

}

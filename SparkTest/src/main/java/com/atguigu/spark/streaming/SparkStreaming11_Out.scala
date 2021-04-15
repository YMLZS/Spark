package com.atguigu.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

/**
  * @author yml
  * 2021-03-20-16:27
  */
object SparkStreaming11_Out {

  def main(args: Array[String]): Unit = {

    //TODO 输出
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

    //自定义输出路径
    count.foreachRDD(
      rdd => {
        //这里的代码在Driver端执行
        //注意：所有的连接对象是不能序列化的，所以如果想保存到MySQL之类的需要连接对象的，需要在Executor中创建连接
      }
    )

    ssc.start()
    ssc.awaitTermination()

  }

}

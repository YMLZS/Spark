package com.atguigu.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

/**
  * @author yml
  * 2021-03-20-16:27
  */
object SparkStreaming01_WordCount {

  def main(args: Array[String]): Unit = {

    //TODO 创建环境对象
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("SparkStreaming")
    //构建StreamingContext，第二个参数表示数据的采集周期，毫秒为单位
    val ssc = new StreamingContext(conf,Duration(3000))
    //可以以秒为单位或者分钟
    //val spark = new StreamingContext(conf,Seconds(3))

    //获取离散化流，监控端口，从Socket中获取的是一行一行的字符串
    val sds: ReceiverInputDStream[String] = ssc.socketTextStream("localhost",9999)
    val words: DStream[String] = sds.flatMap(_.split(" "))
    val count: DStream[(String, Int)] = words.map((_,1)).reduceByKey(_+_)
    count.print()

    //Driver和采集器都要一直执行
    //启动采集器
    ssc.start()
    //Driver等待采集器的结束，awaitTermination会阻塞主线程的运行
    ssc.awaitTermination()

    //SparkStreaming中不能在Driver中执行stop方法
    //ssc.stop()

  }

}

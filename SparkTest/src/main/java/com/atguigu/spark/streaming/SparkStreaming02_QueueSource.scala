package com.atguigu.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Duration, StreamingContext}

import scala.collection.mutable

/**
  * @author yml
  * 2021-03-20-16:27
  */
object SparkStreaming02_QueueSource {

  def main(args: Array[String]): Unit = {

    //TODO 从RDD队列中创建DStream
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("SparkStreaming")
    val ssc = new StreamingContext(conf,Duration(3000))

    val queue = new mutable.Queue[RDD[Int]]
    val ds: InputDStream[Int] = ssc.queueStream(queue,oneAtATime = false)
    ds.print()

    ssc.start()

    for (i <- 1 to 5) {
      queue += ssc.sparkContext.makeRDD(1 to 300, 10)
      Thread.sleep(2000)
    }

    ssc.awaitTermination()

  }

}

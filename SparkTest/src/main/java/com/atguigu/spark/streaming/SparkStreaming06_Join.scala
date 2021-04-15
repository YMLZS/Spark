package com.atguigu.spark.streaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Duration, StreamingContext}

/**
  * @author yml
  * 2021-03-20-16:27
  */
object SparkStreaming06_Join {

  def main(args: Array[String]): Unit = {

    //TODO 双流Join，将相同key的value聚合
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("SparkStreaming")
    val ssc = new StreamingContext(conf,Duration(3000))

    val sds: ReceiverInputDStream[String] = ssc.socketTextStream("localhost",9999)
    val sds1: ReceiverInputDStream[String] = ssc.socketTextStream("localhost",8888)
    val v1: DStream[(String, Int)] = sds.map((_,1))
    val v2: DStream[(String, Int)] = sds1.map((_,1))

    //作用与RDD之间的join相同，要求两个DStream的批次大小必须相同
    val result: DStream[(String, (Int, Int))] = v1.join(v2)

    result.print()

    ssc.start()
    ssc.awaitTermination()

  }

}

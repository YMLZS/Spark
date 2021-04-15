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
object SparkStreaming07_State {

  def main(args: Array[String]): Unit = {

    //TODO 有状态离散化流，用于统计多个周期的计算结果
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("SparkStreaming")
    val ssc = new StreamingContext(conf,Duration(3000))
    //2.缓冲区在检查点中，需要先设置检查点路径，每个周期都会创建一个检查点文件，小文件过多
    ssc.checkpoint("cp")

    val sds: ReceiverInputDStream[String] = ssc.socketTextStream("localhost",9999)

    //统计每个周期内的wordCount
    //val wordCount: DStream[(String, Int)] = sds.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    val ds: DStream[(String, Int)] = sds.flatMap(_.split(" ")).map((_,1))

    //保存所有周期的计算结果，做总的统计
    //1.updateStateByKey用于记录历史，需要传递一个函数，函数的输入为：
    // Seq : 将相同key的value放入seq集合中
    // Option : 缓冲区是否有数据
    val result: DStream[(String, Int)] = ds.updateStateByKey(
      (seq: Seq[Int], buffer: Option[Int]) => {
        val oldCount: Int = buffer.getOrElse(0)
        val newCount = oldCount + seq.sum
        Option(newCount)
      }
    )

    result.print()

    ssc.start()
    ssc.awaitTermination()

  }

}

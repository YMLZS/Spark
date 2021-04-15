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
object SparkStreaming05_Transform {

  def main(args: Array[String]): Unit = {

    //TODO 无状态转换
    //转换：有些方法是DStream流没有的，必须要转换成RDD进行操作(例如sortBy排序)，这时就要使用transform原语
    //无状态：在一个区间内进行转换，不涉及多个区间的统计

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("SparkStreaming")
    val ssc = new StreamingContext(conf,Duration(3000))

    val sds: ReceiverInputDStream[String] = ssc.socketTextStream("localhost",9999)
    val words: DStream[String] = sds.flatMap(_.split(" "))
    val count: DStream[(String, Int)] = words.map((_,1)).reduceByKey(_+_)

    //离散化流无法排序，可以将DStream转换为RDD进行排序
    //Code Driver (只执行一次)
    count.transform( //这个函数会周期性执行
      rdd => {
        //Code Driver (周期性执行)
        rdd.map(_._2) //Code Executor (多少个Task就执行多少次)
      }
    ).print()

    //DStream的方法叫原语，RDD的方法叫算子，Scala对象的方法叫方法
    //count.map(_._2).print()

    //Code Driver (只执行一次)
    count.map(
      kv => {
        //Code Executor (多少个Task就执行多少次)
        kv._2
      }
    )

    ssc.start()
    ssc.awaitTermination()

  }

}

package com.atguigu.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

/**
  * @author yml
  * 2021-03-20-16:27
  */
object SparkStreaming12_Stop {

  def main(args: Array[String]): Unit = {

    //TODO 优雅的关闭
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("SparkStreaming")
    val ssc = new StreamingContext(conf,Duration(3000))
    //
    ssc.checkpoint("cp")

    val sds: ReceiverInputDStream[String] = ssc.socketTextStream("localhost",9999)
    val words: DStream[String] = sds.flatMap(_.split(" "))
    val a: DStream[(String, Int)] = words.map((_, 1))

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

    //start之前不能stop
    ssc.start()
    //刚start就stop也不行

    new Thread(new Runnable {
      override def run(): Unit = {
        //我们需要第三方中间件
        //让这个线程监控一个数据，我们通过操作第三方的数据来控制程序的停止
        //MySQL,HDFS,ZK,Redis等都可以当做中间件
        Thread.sleep(3000)
        //第一个参数为停止环境，第二个参数为优雅停止（false为强制停止）
        //强制停止不好，该为true可以优雅的关闭（先停止接收数据，把已经接收到的数据计算完再关闭）
        ssc.stop(true,false)
      }
    }).start()

    ssc.awaitTermination()
    //在这里写，走不到，awaitTermination会阻塞线程


  }

}

package com.atguigu.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Duration, StreamingContext}

/**
  * @author yml
  * 2021-03-20-16:27
  */
object SparkStreaming08_CP {

  def main(args: Array[String]): Unit = {

    //TODO 从检查点恢复数据
    //一般不使用检查点，因为小文件太多，一般使用Redis存储
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("SparkStreaming")

    val ssc: StreamingContext = StreamingContext.getOrCreate("cp", () => {
      val ssc = new StreamingContext(conf, Duration(3000))
      ssc.checkpoint("cp")
      val sds: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

      val ds: DStream[(String, Int)] = sds.flatMap(_.split(" ")).map((_, 1))

      val result: DStream[(String, Int)] = ds.updateStateByKey(
        (seq: Seq[Int], buffer: Option[Int]) => {
          val oldCount: Int = buffer.getOrElse(0)
          val newCount = oldCount + seq.sum
          Option(newCount)
        }
      )

      result.print()
      ssc
    })

    ssc.start()
    ssc.awaitTermination()

  }

}

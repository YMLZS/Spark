package com.atguigu.spark.core.rddIO

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author yml
  * 2021-03-17-15:23
  */
object Spark01_Save {

  def main(args: Array[String]): Unit = {

    //TODO RDD - IO

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Persist")
    val sc = new SparkContext(conf)

    val rdd: RDD[(String, String)] = sc.makeRDD(
      List(
        ("nba", "XXX"),
        ("cba", "XXX"),
        ("cba", "XXX"),
        ("cba", "XXX"),
        ("nba", "XXX"),
        ("wnba", "XXX")
      ), 3
    )

    rdd.saveAsTextFile("output1")
    rdd.saveAsObjectFile("output2")
    rdd.saveAsSequenceFile("output3")

    sc.stop()

  }

}

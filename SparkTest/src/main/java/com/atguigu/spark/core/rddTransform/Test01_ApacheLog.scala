package com.atguigu.spark.core.rddTransform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * @author yml
  * 2021-03-12-16:03
  */
object Test01_ApacheLog {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Instance")
    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.textFile("data/apache.log")
    val urlRdd: RDD[String] = rdd.map(
      line => {
        line.split(" ")(6)
      }
    )
    urlRdd.collect().foreach(println(_))

    sc.stop()

  }

}

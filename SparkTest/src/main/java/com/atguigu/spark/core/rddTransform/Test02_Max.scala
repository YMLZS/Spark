package com.atguigu.spark.core.rddTransform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author yml
  * 2021-03-12-16:03
  */
object Test02_Max {

  def main(args: Array[String]): Unit = {

    //获取每个分区的最大值

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Instance")
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)
    val rdd1: RDD[Int] = rdd.mapPartitions(
      list => {
        List(list.max).iterator
      }
    )

    rdd1.collect().foreach(println(_))

    sc.stop()

  }

}

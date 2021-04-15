package com.atguigu.spark.core.rddAction

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author yml
  * 2021-03-16-17:02
  */
object Spark09_Action_CountByValue {

  def main(args: Array[String]): Unit = {

    //TODO RDD - 行动算子


    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Transform")
    val sc = new SparkContext(conf)

    //countByValue：统计每个元素的个数
    val rdd: RDD[String] = sc.makeRDD(List("hello","hello","Hello","word"))

    val num: collection.Map[String, Long] = rdd.countByValue()
    println(num)

    sc.stop()

  }

}

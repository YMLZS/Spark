package com.atguigu.spark.core.rddTransform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author yml
  * 2021-03-12-15:36
  */
object Spark11_Transform_Repartition {

  def main(args: Array[String]): Unit = {

    //TODO RDD转换算子

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Transform")
    val sc = new SparkContext(conf)

    //repartition：增加分区
    //底层调用了coalesce实现的
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6),3)
    val rdd1: RDD[Int] = rdd.repartition(4)
    rdd1.saveAsTextFile("output")

    sc.stop()

  }

}

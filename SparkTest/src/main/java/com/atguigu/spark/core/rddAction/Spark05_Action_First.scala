package com.atguigu.spark.core.rddAction

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author yml
  * 2021-03-16-17:02
  */
object Spark05_Action_First {

  def main(args: Array[String]): Unit = {

    //TODO RDD - 行动算子


    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Transform")
    val sc = new SparkContext(conf)

    //first：取第一个元素
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)

    val i: Int = rdd.first()

    sc.stop()

  }

}

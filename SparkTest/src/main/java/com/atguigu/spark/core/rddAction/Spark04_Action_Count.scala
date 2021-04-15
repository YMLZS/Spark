package com.atguigu.spark.core.rddAction

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author yml
  * 2021-03-16-17:02
  */
object Spark04_Action_Count {

  def main(args: Array[String]): Unit = {

    //TODO RDD - 行动算子


    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Transform")
    val sc = new SparkContext(conf)

    //count：返回数据的总数量
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)

    val l: Long = rdd.count()

    sc.stop()

  }

}

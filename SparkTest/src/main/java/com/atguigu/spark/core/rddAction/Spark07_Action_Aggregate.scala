package com.atguigu.spark.core.rddAction

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author yml
  * 2021-03-16-17:02
  */
object Spark07_Action_Aggregate {

  def main(args: Array[String]): Unit = {

    //TODO RDD - 行动算子


    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Transform")
    val sc = new SparkContext(conf)

    //aggregate与aggregateByKey的区别
    //1.aggregateByKey必须处理键值对类型
    //2.aggregate的初始值在分区内和分区间都会参与计算，aggregateByKey的初始值只在分区计算
    val rdd: RDD[Int] = sc.makeRDD(List(4,2,3,1),2)

    rdd.aggregate(5)(_+_,_+_)

    //fold：当aggregate分区内和分区间计算逻辑相同时，可以用fold代替
    rdd.fold(5)(_+_)

    sc.stop()

  }

}

package com.atguigu.spark.core.rddAction

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author yml
  * 2021-03-16-17:02
  */
object Spark02_Action_Reduce {

  def main(args: Array[String]): Unit = {

    //TODO RDD - 行动算子


    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Transform")
    val sc = new SparkContext(conf)

    //reduce：先分区内两两聚合，再进行分区间聚合
    //(1+2)+(3+4)
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)

    rdd.reduce(_+_)

    sc.stop()

  }

}

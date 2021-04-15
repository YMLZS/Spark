package com.atguigu.spark.core.rddAction

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author yml
  * 2021-03-16-17:02
  */
object Spark06_Action_Take {

  def main(args: Array[String]): Unit = {

    //TODO RDD - 行动算子


    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Transform")
    val sc = new SparkContext(conf)

    //take：取指定数量的元素
    val rdd: RDD[Int] = sc.makeRDD(List(4,2,3,1),2)

    rdd.take(3).foreach(println)
    rdd.takeOrdered(3) //先排序再取前3个

    sc.stop()

  }

}

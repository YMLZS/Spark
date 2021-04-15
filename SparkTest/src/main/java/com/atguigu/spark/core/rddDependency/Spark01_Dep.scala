package com.atguigu.spark.core.rddDependency

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author yml
  * 2021-03-17-11:39
  */
object Spark01_Dep {

  def main(args: Array[String]): Unit = {

    //TODO RDD - 依赖关系

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Transform")
    val sc = new SparkContext(conf)

    //查看血缘
    val rdd: RDD[String] = sc.textFile("data/word.txt")
    println(rdd.toDebugString)
    println("*****************")
    val rdd1: RDD[(String, Int)] = rdd.map((_,1))
    println(rdd1.toDebugString)
    println("*****************")
    val rdd2: RDD[(String, Int)] = rdd1.reduceByKey(_+_)
    println(rdd2.toDebugString)
    println("*****************")

    rdd2.collect()

  }

}

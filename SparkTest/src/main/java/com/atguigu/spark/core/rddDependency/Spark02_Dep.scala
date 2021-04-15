package com.atguigu.spark.core.rddDependency

import org.apache.spark.rdd.RDD
import org.apache.spark.{OneToOneDependency, SparkConf, SparkContext}

/**
  * @author yml
  * 2021-03-17-11:39
  */
object Spark02_Dep {

  def main(args: Array[String]): Unit = {

    //TODO RDD - 依赖关系

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Transform")
    val sc = new SparkContext(conf)

    //查看依赖
    val rdd: RDD[String] = sc.textFile("data/word.txt")
    println(rdd.dependencies)
    println("*****************")
    val rdd1: RDD[(String, Int)] = rdd.map((_,1))
    println(rdd1.dependencies)
    println("*****************")
    val rdd2: RDD[(String, Int)] = rdd1.reduceByKey(_+_)
    println(rdd1)
    println(rdd2.dependencies)
    println("*****************")

    rdd2.collect()

  }

}

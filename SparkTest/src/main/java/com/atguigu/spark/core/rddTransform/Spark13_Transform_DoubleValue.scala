package com.atguigu.spark.core.rddTransform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author yml
  * 2021-03-12-15:36
  */
object Spark13_Transform_DoubleValue {

  def main(args: Array[String]): Unit = {

    //TODO RDD转换算子

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Transform")
    val sc = new SparkContext(conf)

    //双值类型的算子
    val rdd1: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)
    val rdd2: RDD[Int] = sc.makeRDD(List(3,4,5,6),2)

    //两个数据集中数据类型必须相同，才能做交集、并集、差集
    //交集
    println(rdd1.intersection(rdd2).collect().mkString(","))
    //并集
    println(rdd1.union(rdd2).collect().mkString(","))
    //差集
    println(rdd1.subtract(rdd2).collect().mkString(","))

    //两个类型不同的数据集可以实现拉链效果，但是两个数据集对应的分区的数据个数必须相同，
      //并且两个数据集的分区数也必须相同
    //拉链
    println(rdd1.zip(rdd2).collect().mkString(","))

    sc.stop()

  }

}

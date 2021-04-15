package com.atguigu.spark.core.rddTransform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author yml
  * 2021-03-12-15:36
  */
object Spark01_Transform_Map {

  def main(args: Array[String]): Unit = {

    //TODO RDD转换算子
    //所谓的算子，就是RDD提供的方法，将一个RDD转换成另外一个RDD的方法，就称之为转换算子
    //RDD分区之间并行计算，分区内串行计算

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Transform")
    val sc = new SparkContext(conf)

    //1.map算子：将数据集中的每条数据进行转换（数值的转换或类型的转换）
    //map算子的参数为函数类型，用法与List.map()相同
    //区别是List.map()是单点计算的，RDD.map()是分布式计算
//    val rdd1: RDD[Int] = sc.makeRDD(List(1,2,3,4))
    val rdd1: RDD[Int] = sc.makeRDD(List(1,2,3,4),2) //通过分区实现并行计算(前提local[*])
    val rdd2: RDD[Int] = rdd1.map(_ * 2)
    println(rdd2.partitions.length) //默认转换前后RDD的分区数量不变，如果分区数不变，那么数据所在的分区也不变
    rdd2.collect().foreach(println(_))
    //如果有多个转换算子，那么一条数据会执行完所有算子，下一条数据才会进来（所以说RDD类似管道），但是这样效率较低
    //为了提高效率，使用mapPartitions()，以分区为单位进行转换

    //mapPartitions算子：将数据以分区为单位进行转换，类似于批处理，效率高


    sc.stop()

  }

}

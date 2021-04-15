package com.atguigu.spark.core.rddTransform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author yml
  * 2021-03-12-15:36
  */
object Spark02_Transform_MapPartitions {

  def main(args: Array[String]): Unit = {

    //TODO RDD转换算子
    //map与mapPartitions的区别
    //1.mapPartitions为批处理，效率高
    //2.mapPartitions在内存有限的情况下，可能会出现内存溢出
    //3.mapPartitions可以改变元素的个数，只要返回迭代器即可

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Transform")
    val sc = new SparkContext(conf)

    //mapPartitions算子：将数据以分区为单位进行转换，类似于批处理，效率高
    //参数为函数，函数的参数为每个分区的迭代器，一个分区一个迭代器，也需要返回迭代器
    val rdd1: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)

    val rdd2: RDD[Int] = rdd1.map(
      num => {
        println("********")
        num * 2
      }
    )

    rdd2.collect().foreach(println(_))

    val rdd3: RDD[Int] = rdd1.mapPartitions(
      list => {
        println("---------")
        list.map(_ * 2)
      }
    )

    rdd3.collect().foreach(println(_))

    //mapPartitions将分区数据作为整体来处理，就需要内存中的数据有限制，否则会出现内存溢出
    //因为mapPartitions处理的数据不会马上释放，会等待整个分区的数据全部处理完才会释放

    sc.stop()

  }

}

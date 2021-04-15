package com.atguigu.spark.core.rddTransform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author yml
  * 2021-03-12-15:36
  */
object Spark07_Transform_Filter {

  def main(args: Array[String]): Unit = {

    //TODO RDD转换算子

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Transform")
    val sc = new SparkContext(conf)

    //filter：去除不要的元素，根据规则对每个元素进行判断，返回true为保留数据，false为丢弃数据
    //Scala中的filter考虑单点操作
    //Spark中的filter考虑数据均衡问题，过滤后分区不变（没有shuffle），但是可能会出现数据倾斜
    //mapPartitions、mapPartitionWithIndex、flatMap都可以改变数据的数量
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))
    val rdd1: RDD[Int] = rdd.filter(
      num => {
        if (num % 2 == 0) {
          false
        } else {
          true
        }
      }
    )
    rdd1.collect().foreach(println(_))

    sc.stop()

  }

}

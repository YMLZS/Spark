package com.atguigu.spark.core.rddTransform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author yml
  * 2021-03-12-15:36
  */
object Spark10_Transform_Coalesce {

  def main(args: Array[String]): Unit = {

    //TODO RDD转换算子

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Transform")
    val sc = new SparkContext(conf)

    //coalesce：缩减分区
    //第一个参数表示改变分区数量，第二个参数（可选）表示数据是否shuffle
    //合并的策略：
      //如果不传第二个参数，只考虑分区之间的远近，可能会有数据倾斜
      //如果传入第二个参数，所有数据进行shuffle，可以做到数据均衡
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6),3)
    //val rdd1: RDD[Int] = rdd.coalesce(2)
    val rdd1: RDD[Int] = rdd.coalesce(2,true)
    rdd1.saveAsTextFile("output")

    //没有shuffle的情况下，分区数不能设置为比之前更大的值，因为数据没有打乱
    //所以coalesce也可以增加分区，必须用shuffle

    sc.stop()

  }

}

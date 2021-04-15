package com.atguigu.spark.core.rddTransform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author yml
  * 2021-03-12-15:36
  */
object Spark03_Transform_MapPartitionsWithIndex {

  def main(args: Array[String]): Unit = {

    //TODO RDD转换算子

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Transform")
    val sc = new SparkContext(conf)

    //mapPartitionsWithIndex第一个参数为分区号
    //分区号从0开始
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6),3)
    val newRdd: RDD[Int] = rdd.mapPartitionsWithIndex(
      (index, iter) => {
        if (index == 1) {
          iter.map(_ * 4)
        }else{
          iter
          //Nil.iterator //返回空集合（只留下index=1的分区）
        }
      }
    )

    newRdd.collect().foreach(println(_))

    sc.stop()

  }

}

package com.atguigu.spark.core.rddTransform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author yml
  * 2021-03-12-15:36
  */
object Spark12_Transform_SortBy {

  def main(args: Array[String]): Unit = {

    //TODO RDD转换算子

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Transform")
    val sc = new SparkContext(conf)

    //sortBy：全局排序，有shuffle，可以重新指定分区数
    val rdd: RDD[Int] = sc.makeRDD(List(1,3,2,7,6),2)
    rdd.saveAsTextFile("output1")
    rdd.sortBy(num=>num).saveAsTextFile("output2")
    rdd.sortBy(num=>num,true) //升序
    rdd.sortBy(num=>num,false) //降序

    sc.stop()

  }

}

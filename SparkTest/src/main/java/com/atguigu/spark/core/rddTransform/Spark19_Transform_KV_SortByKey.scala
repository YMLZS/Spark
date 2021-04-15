package com.atguigu.spark.core.rddTransform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author yml
  * 2021-03-12-15:36
  */
object Spark19_Transform_KV_SortByKey {

  def main(args: Array[String]): Unit = {

    //TODO RDD转换算子

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Transform")
    val sc = new SparkContext(conf)

    //sortByKey：对key进行排序，key必须实现Ordered特质并实现compare方法
    //底层进行了隐式转换，用到了Ordered特质的compare实现方法
    val rdd = sc.makeRDD(
      List((2,1),(3,1),(1,1),(1,3),(1,2)),
      2)

    val rdd1 = rdd.sortByKey(true)

    rdd1.collect().foreach(println(_))

    sc.stop()

  }

}

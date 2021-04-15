package com.atguigu.spark.core.rddTransform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author yml
  * 2021-03-12-15:36
  */
object Spark20_Transform_KV_Join {

  def main(args: Array[String]): Unit = {

    //TODO RDD转换算子
    //join有时会shuffle，有时不会
    //join算子的依赖都是OneToOne

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Transform")
    val sc = new SparkContext(conf)

    //join：两个数据集中如果有相同的key，对他们的value进行连接
    //join不考虑数量问题，数据集1中的每个数据都会试图匹配数据集2中的每个数据
      //极端情况下，会出现笛卡尔积，导致资源耗尽，尽量不要用join
    val rdd = sc.makeRDD(
      List(("a",1),("b",2),("c",3)))
    val rdd1 = sc.makeRDD(
      List(("a",4),("d",5),("c",6),("a",2)))

    val rdd2: RDD[(String, (Int, Int))] = rdd.join(rdd1)

    rdd2.collect().foreach(println(_))

    sc.stop()

  }

}

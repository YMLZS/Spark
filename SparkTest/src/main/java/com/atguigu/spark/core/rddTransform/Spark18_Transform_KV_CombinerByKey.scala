package com.atguigu.spark.core.rddTransform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * @author yml
  * 2021-03-12-15:36
  */
object Spark18_Transform_KV_CombinerByKey {

  def main(args: Array[String]): Unit = {

    //TODO RDD转换算子

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Transform")
    val sc = new SparkContext(conf)

    //combinerByKey：分组聚合
    //特点：可以对初始数据进行转换，分组内和分组间的计算逻辑可以单独定义
    //第一个参数：初始值的转换
    //第二个参数：分区内计算逻辑
    //第三个参数：分区间计算逻辑

    //需求：求相同key分区间的平均值
    val rdd: RDD[(String, Int)] = sc.makeRDD(
      List(("a",1),("b",3),("a",2),("a",4),("b",5),("b",6)),
      2)

    val rdd1: RDD[(String, (Int, Int))] = rdd.combineByKey(
      (num: Int) => (num, 1),
      (t: (Int, Int), num: Int) => (t._1 + num, t._2 + 1),
      (t1: (Int, Int), t2: (Int, Int)) => (t1._1 + t2._1, t1._2 + t2._2)
    )

    rdd1.map(
      t => (t._1,t._2._1 / t._2._2)
    ).collect().foreach(println)

    rdd1.collect().foreach(println(_))

    sc.stop()

  }

}

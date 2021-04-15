package com.atguigu.spark.core.rddTransform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * @author yml
  * 2021-03-12-15:36
  */
object Spark17_Transform_KV_AggregateByKey {

  def main(args: Array[String]): Unit = {

    //TODO RDD转换算子

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Transform")
    val sc = new SparkContext(conf)

    //aggregate：聚合
    //特点：分区内和分区间的计算逻辑可以单独定义
    //有shuffle
    //aggregateByKey：将相同key的数据，对value进行聚合（分组聚合）
    //aggregateByKey与reduceByKey的区别：
    //1.reduceByKey的分区内和分区间的计算逻辑只能是一致的
    //2.aggregateByKey的分区内和分区间的计算逻辑是可以单独定义的
    //3.aggregateByKey有计算初始值的概念

    //需求：求相同key的分区内最大值，分区间求和
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("b",3),("a",2),("a",4),("b",5),("b",6)),2)
    //有函数的柯里化操作，有多个参数列表
    //第一个参数列表有一个参数，这个参数表示计算初始值
      //计算初始值的作用：("a",1)("a",0) -> ("a",1)
                      //("a",1)("a",2) -> ("a",2)
    //第二个参数列表有两个参数：
      //第一个参数表示分区内计算逻辑
      //第二个参数表示分区间计算逻辑
    val rdd1: RDD[(String, Int)] = rdd.aggregateByKey(0)(
      (x, y) => math.max(x, y), //分区内求最大值
      (x, y) => x + y //分区间求和
    )
    rdd1.collect().foreach(println(_))

    //当aggregateByKey的分区内和分区间计算相同时，可以用foldByKey代替
    rdd.foldByKey(0)(_+_)

    sc.stop()

  }

}

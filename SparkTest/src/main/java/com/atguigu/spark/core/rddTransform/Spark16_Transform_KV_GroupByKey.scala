package com.atguigu.spark.core.rddTransform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * @author yml
  * 2021-03-12-15:36
  */
object Spark16_Transform_KV_GroupByKey {

  def main(args: Array[String]): Unit = {

    //TODO RDD转换算子

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Transform")
    val sc = new SparkContext(conf)

    //groupByKey：将相同key的数据分为一组
    //一定有shuffle
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("b",2),("a",3)),2)
    //两种方法得到的结果稍有区别
    val rdd1: RDD[(String, Iterable[(String, Int)])] = rdd.groupBy(_._1)
    val rdd2: RDD[(String, Iterable[Int])] = rdd.groupByKey()

//    rdd1.collect().foreach(println(_))
//    rdd2.collect().foreach(println(_))
    rdd2.saveAsTextFile("output")

    sc.stop()

  }

}

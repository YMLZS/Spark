package com.atguigu.spark.core.rddAction

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author yml
  * 2021-03-16-17:02
  */
object Spark08_Action_CountByKey {

  def main(args: Array[String]): Unit = {

    //TODO RDD - 行动算子


    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Transform")
    val sc = new SparkContext(conf)

    //countByKey：统计key相同的元素个数
    val rdd: RDD[(Int, Int)] = sc.makeRDD(List((1,1),(1,2),(1,3),(2,1)),2)

    val num: collection.Map[Int, Long] = rdd.countByKey()
    println(num)

    sc.stop()

  }

}

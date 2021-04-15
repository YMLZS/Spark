package com.atguigu.spark.core.rddTransform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author yml
  * 2021-03-12-15:36
  */
object Spark05_Transform_Glom {

  def main(args: Array[String]): Unit = {

    //TODO RDD转换算子

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Transform")
    val sc = new SparkContext(conf)

    //glom：将每个分区转换为数组返回
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)
    val glom: RDD[Array[Int]] = rdd.glom
//    val glom: RDD[Array[Int]] = rdd.glom() glom的参数列表没有参数，加不加小括号都可以
    glom.collect().foreach(println(_))

    //取每个分区的最大值
    val max: RDD[Int] = glom.map(_.max)
    max.collect().foreach(println(_))

    //需求：求每个分区的最大值，求分区间的最大值之和
    val maxRDD: RDD[Int] = rdd.glom().map(_.max)
    println(maxRDD.collect().sum) //collect将结果用数组返回

    sc.stop()

  }

}

package com.atguigu.spark.core.rddAction

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author yml
  * 2021-03-16-17:02
  */
object Spark01_Action {

  def main(args: Array[String]): Unit = {

    //TODO RDD - 行动算子
    //行动算子：可以触发RDD执行的算子
    //Spark中，调用行动算子，会执行作业，每一次调用，执行的作业都不同

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Transform")
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))
    val rdd1: RDD[Int] = rdd.map(
      num => {
        //如果没有行动算子，那么RDD不会执行，数据不会流过RDD管道
        //println(num)
        num * 2
      }
    )

    //collect算子表示收集结果
    //这个foreach是Array的方法，串行打印
    rdd1.collect().foreach(println(_))
    println("*************************")
    //这个foreach是RDD的算子，并行打印（如果改为local，也会串行打印）
    rdd1.foreach(println(_))

    sc.stop()

  }

}

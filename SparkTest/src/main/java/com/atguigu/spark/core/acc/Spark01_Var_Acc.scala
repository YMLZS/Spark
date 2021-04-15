package com.atguigu.spark.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author yml
  * 2021-03-17-16:26
  */
object Spark01_Var_Acc {

  def main(args: Array[String]): Unit = {

    //TODO 累加器

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Acc")
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6),2)

    var sum = 0

    //闭包，把sum传过去了
    rdd.foreach(
      num => sum = sum + num
    )

    //但是sum没有返回来，RDD无法返回自定义的操作
    println(sum) //0

    sc.stop()

  }

}

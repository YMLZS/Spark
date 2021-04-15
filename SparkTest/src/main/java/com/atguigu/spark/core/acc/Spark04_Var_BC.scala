package com.atguigu.spark.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author yml
  * 2021-03-17-16:26
  */
object Spark04_Var_BC {

  def main(args: Array[String]): Unit = {

    //TODO 累加器

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Acc")
    val sc = new SparkContext(conf)

    //笛卡尔积
    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("a",2),("a",3)),2)
    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a",4),("a",5),("a",6)),2)

    val rdd3: RDD[(String, (Int, Int))] = rdd1.join(rdd2)

    rdd3.collect().foreach(println)

    sc.stop()

  }

}

package com.atguigu.spark.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  * @author yml
  * 2021-03-20-16:27
  */
object SparkSQL01_Environment {

  def main(args: Array[String]): Unit = {

    //TODO 创建环境对象
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("WordCount")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()


    spark.stop()

  }

}

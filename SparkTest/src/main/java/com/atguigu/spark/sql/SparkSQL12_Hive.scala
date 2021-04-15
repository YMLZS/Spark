package com.atguigu.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql._

/**
  * @author yml
  * 2021-03-20-16:27
  */
object SparkSQL12_Hive {

  def main(args: Array[String]): Unit = {

    //TODO 与外置Hive集成
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("WordCount")
    val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()

    spark.sql("show tables").show


    spark.stop()

  }

}

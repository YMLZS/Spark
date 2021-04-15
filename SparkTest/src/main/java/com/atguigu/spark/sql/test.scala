package com.atguigu.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @author yml
  * 2021-03-22-20:12
  */
object test {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
    val ss: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val df: DataFrame = ss.read.json("data/myJson.json")
    df.show()

    ss.stop()

  }

}

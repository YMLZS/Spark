package com.atguigu.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @author yml
  * 2021-03-20-16:27
  */
object SparkSQL02_SQL {

  def main(args: Array[String]): Unit = {

    //TODO SQL操作
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("WordCount")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //JSON文件对格式有要求
    //但SparkSQL底层是SparkCore，SparkCore是基于Hadoop的，Hadoop是按行读取文件的
    //所以，SparkSQL读取JSON文件要求每行单独符合JSON格式
    val df: DataFrame = spark.read.json("data/myJson.json")

    //将DataFrame转换为临时视图
    df.createOrReplaceTempView("user")

    //使用SQL访问视图
    spark.sql("select * from user").show()


    spark.stop()

  }

}

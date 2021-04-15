package com.atguigu.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Aggregator

/**
  * @author yml
  * 2021-03-20-16:27
  */
object SparkSQL11_SaveAndLoadMySQL {

  def main(args: Array[String]): Unit = {

    //TODO UDAF - 用户自定义聚合函数
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("WordCount")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    //从MySQL中读取数据
    val df: DataFrame = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/bookproject")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "root")
      .option("dbtable", "t_user")
      .load()

    //保存数据到MySQL中
    df.write.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/test")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "root")
      .option("dbtable", "temp")
      .save()

    spark.stop()

  }

}

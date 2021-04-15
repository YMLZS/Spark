package com.atguigu.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @author yml
  * 2021-03-20-16:27
  */
object SparkSQL03_DSL {

  def main(args: Array[String]): Unit = {

    //TODO DSL操作
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("WordCount")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val df: DataFrame = spark.read.json("data/myJson.json")

    df.select("username","age").show()

    //在使用特殊操作时，需要隐式转换操作，在shell窗口是自动导入的，但在这里需要手动导入
    //import导入的是对象，而且此对象必须是val的
    import spark.implicits._ //这里的spark不是包名，是前面的对象
    df.select($"age" + 10).show()
    df.select('age + 10).show()

    spark.stop()

  }

}

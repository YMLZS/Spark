package com.atguigu.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * @author yml
  * 2021-03-20-16:27
  */
object SparkSQL05_DataFrameAndDataset {

  def main(args: Array[String]): Unit = {

    //TODO DataFrame和Dataset的关系
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("WordCount")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    //RDD
    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(
      List(
        (1, "zhangsan", 30),
        (2, "lisi", 40),
        (3, "wangwu", 50)
      )
    )

    val df: DataFrame = rdd.toDF("id","name","age")

    val ds: Dataset[Person] = df.as[Person]

    //DataFrame是绿色的，表示是别名
    //DataFrame是Dataset[ROW]的别名
    //DataFrame的本质就是Dataset

    spark.stop()

  }

  case class Person(id:Int,name:String,age:Int)

}

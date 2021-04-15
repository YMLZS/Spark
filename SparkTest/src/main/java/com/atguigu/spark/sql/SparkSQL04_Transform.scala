package com.atguigu.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * @author yml
  * 2021-03-20-16:27
  */
object SparkSQL04_Transform {

  def main(args: Array[String]): Unit = {

    //TODO RDD,DataFrame,Dataset的转换
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

    //RDD => DataFrame
    val df: DataFrame = rdd.toDF("id","name","age")
    df.printSchema()

    //DataFrame => Dataset
    val ds: Dataset[Person] = df.as[Person]

    //Dataset => DataFrame
    ds.toDF()

    //DataFrame => RDD
    val rdd1: RDD[Row] = df.rdd

    //RDD => Dataset
    rdd.map{
      case (id,name,age) => Person(id,name,age)
    }.toDS()

    //Dataset => RDD
    val rdd2: RDD[Person] = ds.rdd

    spark.stop()

  }

  case class Person(id:Int,name:String,age:Int)

}

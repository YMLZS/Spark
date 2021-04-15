package com.atguigu.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * @author yml
  * 2021-03-20-16:27
  */
object SparkSQL06_UDF {

  def main(args: Array[String]): Unit = {

    //TODO UDF - 用户自定义函数
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("WordCount")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(
      List(
        (1, "zhangsan", 30),
        (2, "lisi", 40),
        (3, "wangwu", 50)
      )
    )

    val df: DataFrame = rdd.toDF("id","name","age")
    df.createOrReplaceTempView("user")

    //需求：查询name列，将结果的首字母变成大写返回
    //将查询结果的每一条经过UDF函数处理
    def headUpper(name : String) = {
      name.head.toUpper + name.tail
    }

    //spark将sql解析为rdd
    //一般来说，UDF用匿名函数声明
    //在Spark中注册，让Spark知道
    spark.udf.register("xx",(name:String)=>{name.head.toUpper + name.tail})

    spark.sql("select xx(name) from user").show()

    spark.stop()

  }

}

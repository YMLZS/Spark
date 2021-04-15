package com.atguigu.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, IntegerType, StructField, StructType}
import org.apache.spark.sql._

/**
  * @author yml
  * 2021-03-20-16:27
  */
object SparkSQL08_UDAF {

  def main(args: Array[String]): Unit = {

    //TODO UDAF - 用户自定义聚合函数
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

    //需求：求age的平均值
    //将查询结果的每一条经过UDAF函数处理
    val udaf = new MyUDAF
    spark.udf.register("xx",functions.udaf(udaf))
    //sql中不支持强类型聚合函数
    //在Spark3.0版本后，可以通过特殊操作将强类型转换为弱类型
    spark.sql("select xx(age) from user").show()

    spark.stop()

  }

  case class AvgBuffer(var total:Int,var count:Int)

  //自定义UDAF
  //1.继承Aggregator - 强类型的聚合函数
  //2.定义泛型
    //IN : age - Int
    //BUF : case class 或者 (Int,Int)
    //OUT : avg - Int
  //3.重写方法
  class MyUDAF extends Aggregator[Int,AvgBuffer,Int] {
    //初始化缓冲区
    override def zero: AvgBuffer = {
      AvgBuffer(0,0)
    }

    //更新缓冲区
    override def reduce(b: AvgBuffer, a: Int): AvgBuffer = {
      b.total += a
      b.count += 1
      b
    }

    //合并缓冲区
    override def merge(b1: AvgBuffer, b2: AvgBuffer): AvgBuffer = {
      b1.count += b2.count
      b1.total += b2.total
      b1
    }

    //计算
    override def finish(reduction: AvgBuffer): Int = {
      reduction.total / reduction.count
    }

    //编码
    override def bufferEncoder: Encoder[AvgBuffer] = Encoders.product

    //解码
    override def outputEncoder: Encoder[Int] = Encoders.scalaInt
  }

}

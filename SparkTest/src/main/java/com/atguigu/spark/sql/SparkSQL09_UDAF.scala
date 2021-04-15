package com.atguigu.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Aggregator

/**
  * @author yml
  * 2021-03-20-16:27
  */
object SparkSQL09_UDAF {

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

    val udaf = new MyUDAF
    //Spark3.0之前也有强类型，但是不能在sql中使用
    //早期版本中使用DSL来使用强类型，输入的内容是一行内容
    val ds: Dataset[User] = df.as[User]
    //强类型聚合函数可以转换为查询列进行操作

    ds.select(udaf.toColumn).show()

    spark.stop()

  }

  case class AvgBuffer(var total:Int,var count:Int)
  case class User(id:Int,name:String,age:Int)

  class MyUDAF extends Aggregator[User,AvgBuffer,Int] {
    //初始化缓冲区
    override def zero: AvgBuffer = {
      AvgBuffer(0,0)
    }

    //更新缓冲区
    override def reduce(b: AvgBuffer, a: User): AvgBuffer = {
      b.total += a.age
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

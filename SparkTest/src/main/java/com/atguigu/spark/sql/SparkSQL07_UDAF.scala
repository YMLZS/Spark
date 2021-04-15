package com.atguigu.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * @author yml
  * 2021-03-20-16:27
  */
object SparkSQL07_UDAF {

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
    spark.udf.register("xx",new MyUDAF)

    spark.sql("select xx(age) from user").show()

    spark.stop()

  }

  //自定义UDAF
  //1.继承UserDefinedAggregateFunction - 弱类型的聚合函数
  //2.重写方法
  class MyUDAF extends UserDefinedAggregateFunction{
    //输入数据的结构
    override def inputSchema: StructType = {
      StructType(
        Array(
          StructField("age",IntegerType)
        )
      )
    }

    //缓冲区数据的结构
    override def bufferSchema: StructType = {
      StructType(
        Array(
          StructField("total",IntegerType),
          StructField("count",IntegerType)
        )
      )
    }

    //输出数据的类型
    override def dataType: DataType = IntegerType

    //数据的稳定性（输入相同参数时，输出结果是否一样）
    override def deterministic: Boolean = true

    //初始化缓冲区（初始化缓冲区中的数据初始值）
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer.update(0,0) //将total设置为0
      buffer.update(1,0) //将count设置为0
    }

    //更新缓冲区
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      //获取输入进缓冲区的数据，这里只有age一个
      val inputAge: Int = input.getInt(0)
      val bufferAge: Int = buffer.getInt(0)
      val bufferCount: Int = buffer.getInt(1)
      buffer.update(0,inputAge + bufferAge)
      buffer.update(1,bufferCount + 1)
    }

    //合并多个缓冲区（分布式的）
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      //MutableAggregationBuffer继承于Row，MutableAggregationBuffer是可以更新的，有update方法
      //这里作者想让开发人员只更新一个buffer1，buffer2是不能更新的
      val inputAge: Int = buffer2.getInt(0)
      val inputCount: Int = buffer2.getInt(1)
      val bufferAge: Int = buffer1.getInt(0)
      val bufferCount: Int = buffer1.getInt(1)
      buffer1.update(0,inputAge + bufferAge)
      buffer1.update(1,bufferCount + inputCount)
    }

    //计算
    override def evaluate(buffer: Row): Any = {
      buffer.getInt(0) / buffer.getInt(1)
    }
  }

}

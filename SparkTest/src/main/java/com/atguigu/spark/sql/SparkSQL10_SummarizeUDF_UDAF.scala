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
object SparkSQL10_SummarizeUDF_UDAF {

  def main(args: Array[String]): Unit = {

    //TODO 创建环境对象
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("test")
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

    //UDF
//    spark.udf.register("xx",(name:String) => name.head.toUpper + name.tail)
//    spark.sql("select xx(name) from user").show()

    //UDAF - 弱类型
//    spark.udf.register("xx",new MyUDAF)
//    spark.sql("select xx(age) from user").show()

    //UDAF - 强类型 - 3.0版本之后(可以用在SQL中，每次输入的内容是一个单元格)
    val udaf = new MyUDAFPro
    spark.udf.register("xx",functions.udaf(udaf))
    spark.sql("select xx(age) from user").show()

    //UDAF - 强类型 - 3.0版本之前(不可用在SQL中，需要用DSL操作，每次输入的内容是一行)
//    val udaf = new MyUDAFProBefore
//    val ds: Dataset[User] = df.as[User]
//    ds.select(udaf.toColumn).show()

    spark.stop()

  }

  case class User(id:Int,name:String,age:Int)
  case class Buffer(var total:Int,var count:Int)

  class MyUDAF extends UserDefinedAggregateFunction{
    override def inputSchema: StructType = {
      StructType(
        Array(
          StructField("age",IntegerType)
        )
      )
    }

    override def bufferSchema: StructType = {
      StructType(
        Array(
          StructField("total",IntegerType),
          StructField("count",IntegerType)
        )
      )
    }

    override def dataType: DataType = IntegerType

    override def deterministic: Boolean = true

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer.update(0,0)
      buffer.update(1,0)
    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      val total: Int = buffer.getInt(0)
      val count: Int = buffer.getInt(1)
      val age: Int = input.getInt(0)
      buffer.update(0,total + age)
      buffer.update(1,count + 1)
    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      val b1t: Int = buffer1.getInt(0)
      val b1c: Int = buffer1.getInt(1)
      val b2t: Int = buffer2.getInt(0)
      val b2c: Int = buffer2.getInt(1)
      buffer1.update(0,b1t + b2t)
      buffer1.update(1,b1c + b2c)
    }

    override def evaluate(buffer: Row): Any = {
      buffer.getInt(0) / buffer.getInt(1)
    }
  }

  class MyUDAFPro extends Aggregator[Int,Buffer,Int] {
    override def zero: Buffer = Buffer(0,0)

    override def reduce(b: Buffer, a: Int): Buffer = {
      b.total += a
      b.count += 1
      b
    }

    override def merge(b1: Buffer, b2: Buffer): Buffer = {
      b1.total += b2.total
      b1.count += b2.count
      b1
    }

    override def finish(reduction: Buffer): Int = {
      reduction.total / reduction.count
    }

    override def bufferEncoder: Encoder[Buffer] = Encoders.product

    override def outputEncoder: Encoder[Int] = Encoders.scalaInt
  }

  class MyUDAFProBefore extends Aggregator[User,Buffer,Int] {
    override def zero: Buffer = Buffer(0,0)

    override def reduce(b: Buffer, a: User): Buffer = {
      b.total += a.age
      b.count += 1
      b
    }

    override def merge(b1: Buffer, b2: Buffer): Buffer = {
      b1.total += b2.total
      b1.count += b2.count
      b1
    }

    override def finish(reduction: Buffer): Int = {
      reduction.total / reduction.count
    }

    override def bufferEncoder: Encoder[Buffer] = Encoders.product

    override def outputEncoder: Encoder[Int] = Encoders.scalaInt
  }

}

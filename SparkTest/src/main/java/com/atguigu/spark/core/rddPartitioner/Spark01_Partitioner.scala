package com.atguigu.spark.core.rddPartitioner

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
  * @author yml
  * 2021-03-17-15:23
  */
object Spark01_Partitioner {

  def main(args: Array[String]): Unit = {

    //TODO RDD - 自定义分区器
    //1.继承Spark中的Partitioner抽象类
    //2.实现2个抽象方法

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Persist")
    val sc = new SparkContext(conf)

    val rdd: RDD[(String, String)] = sc.makeRDD(
      List(
        ("nba", "XXX"),
        ("cba", "XXX"),
        ("cba", "XXX"),
        ("cba", "XXX"),
        ("nba", "XXX"),
        ("wnba", "XXX")
      ), 3
    )

    //需求：0 - nba，1 - cba，2 - wnba
    val rdd1: RDD[(String, String)] = rdd.partitionBy(new MyPartitioner)

    rdd1.saveAsTextFile("output")

    sc.stop()

  }

  class MyPartitioner extends Partitioner {
    //指定分区数量
    override def numPartitions: Int = {
      3
    }

    //根据key分配分区编号
    override def getPartition(key: Any): Int = {
      key match {
        case "nba" => 0
        case "cba" => 1
        case "wnba" => 2
      }
    }
  }

}

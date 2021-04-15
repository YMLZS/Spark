package com.atguigu.spark.core.rddTransform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * @author yml
  * 2021-03-12-16:03
  */
object Test04_AgentLogPro {

  def main(args: Array[String]): Unit = {

    //获取每个省份，广告点击量的前三名
    //先聚合再分组效率高
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("AgentLog")
    val sc = new SparkContext(conf)

    val rdd1: RDD[String] = sc.textFile("data/agent.log")

    val rdd2: RDD[(String, Int)] = rdd1.map(
      str => {
        val s: Array[String] = str.split(" ")
        (s(1) + "-" + s(4), 1)
      }
    )

    val rdd3: RDD[(String, Int)] = rdd2.reduceByKey(_ + _)

    val rdd4: RDD[(String, (String, Int))] = rdd3.map(
      t => {
        val s: Array[String] = t._1.split("-")
        (s(0), (s(1), t._2))
      }
    )

//    //问题一：groupByKey数据量大、性能低
//    val rdd5: RDD[(String, Iterable[(String, Int)])] = rdd4.groupByKey()
//
//    //问题二：_.toList.sortBy是集合的方法，单点操作，内存可能会溢出
//    val rdd6: RDD[(String, List[(String, Int)])] = rdd5.mapValues(
//      _.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
//    )

    //现在分区内进行排序，并取前三，然后再分区间进行排序，取前三
    //解决了单点资源不足的问题
    val rdd5: RDD[(String, ArrayBuffer[(String, Int)])] = rdd4.aggregateByKey(ArrayBuffer[(String, Int)]())(
      (buffer, t) => {
        buffer.append(t)
        buffer.sortBy(_._2)(Ordering.Int.reverse).take(3)
      },
      (buffer1, buffer2) => {
        buffer1.appendAll(buffer2)
        buffer1.sortBy(_._2)(Ordering.Int.reverse).take(3)
      }
    )

    rdd5.saveAsTextFile("output")
    rdd5.collect().foreach(println(_))

    sc.stop()

  }

}

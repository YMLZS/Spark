package com.atguigu.spark.core.requirement

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author yml
  * 2021-03-19-11:31
  */
object Spark01_HotCategoryTop10 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Req")
    val sc = new SparkContext(conf)

    //1.读取数据
    val rdd: RDD[String] = sc.textFile("data/user_visit_action.txt")

    //2.统计点击数量
    val clickRDD: RDD[String] = rdd.filter(
      line => {
        val datas: Array[String] = line.split("_")
        datas(6) != "-1"
      }
    )
    val clickCountRDD: RDD[(String, Int)] = clickRDD.map(
      line => {
        val datas: Array[String] = line.split("_")
        (datas(6), 1)
      }
    ).reduceByKey(_ + _)

    //3.统计下单数量
    val orderRDD: RDD[String] = rdd.filter(
      line => {
        val datas: Array[String] = line.split("_")
        datas(8) != "null"
      }
    )
    val orderCountRDD: RDD[(String, Int)] = orderRDD.flatMap(
      line => {
        val datas: Array[String] = line.split("_")
        val orderID = datas(8).split(",")
        orderID.map((_, 1))
      }
    ).reduceByKey(_ + _)

    //4.统计支付数量
    val payRDD: RDD[String] = rdd.filter(
      line => {
        val datas: Array[String] = line.split("_")
        datas(10) != "null"
      }
    )
    val payCountRDD: RDD[(String, Int)] = payRDD.flatMap(
      line => {
        val datas: Array[String] = line.split("_")
        val orderID = datas(10).split(",")
        orderID.map((_, 1))
      }
    ).reduceByKey(_ + _)

    //5.将统计结果排序（点击，下单，支付），Tuple的排序是可以有多级排序的
    val count1: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] = clickCountRDD.cogroup(orderCountRDD,payCountRDD)
    val count2: RDD[(String, (Int, Int, Int))] = count1.map {
      case (cat, (click, order, pay)) => {
        var clickCount = 0
        var orderCount = 0
        var payCount = 0
        val it1: Iterator[Int] = click.iterator
        if (it1.hasNext) {
          clickCount = it1.next()
        }
        val it2: Iterator[Int] = order.iterator
        if (it2.hasNext) {
          orderCount = it2.next()
        }
        val it3: Iterator[Int] = pay.iterator
        if (it3.hasNext) {
          payCount = it3.next()
        }
        (cat, (clickCount, orderCount, payCount))
      }
    }

    //6.将结果取前10条
    val result: Array[(String, (Int, Int, Int))] = count2.sortBy(_._2,false).take(10)

    //7.打印结果
    result.foreach(println(_))

    sc.stop()

  }

}

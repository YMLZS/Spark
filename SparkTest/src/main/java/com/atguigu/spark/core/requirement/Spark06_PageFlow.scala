package com.atguigu.spark.core.requirement

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * @author yml
  *         2021-03-19-11:31
  */
object Spark06_PageFlow {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Req")
    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.textFile("data/user_visit_action.txt")
    val actionDatas = rdd.map(
      line => {
        val datas = line.split("_")
        UserVisitAction(
          datas(0),
          datas(1).toLong,
          datas(2),
          datas(3).toLong,
          datas(4),
          datas(5),
          datas(6).toLong,
          datas(7).toLong,
          datas(8),
          datas(9),
          datas(10),
          datas(11),
          datas(12).toLong
        )
      }
    )
    actionDatas.cache()

    val map: Map[Long, Int] = actionDatas.map(
      obj => {
        (obj.page_id, 1)
      }
    ).reduceByKey(_ + _).collect().toMap

    val r1: RDD[(String, Iterable[UserVisitAction])] = actionDatas.groupBy(_.session_id)

    val r2: RDD[(String, List[(String, Int)])] = r1.mapValues(
      iter => {
        val list: List[UserVisitAction] = iter.toList
        val actions: List[UserVisitAction] = list.sortBy(_.action_time)
        val longs: List[Long] = actions.map(_.page_id)
        val tuples: List[(Long, Long)] = longs.zip(longs.tail)
        //(session,List((页面1,页面2),(页面2,页面3)...))
        tuples.map {
          case (a, b) => {
            (a + "-" + b, 1)
          }
        }
      }
    )

    val r3: RDD[(String, Int)] = r2.flatMap(
      t => {
        t._2
      }
    ).reduceByKey(_ + _)


    r3.foreach(
      t => {
        val s: Array[String] = t._1.split("-")
        val l: Int = map.getOrElse(s(0).toLong,1)
        println(t._2.toDouble / l)
      }
    )

    sc.stop()

  }

  case class UserVisitAction(
        date: String, //用户点击行为的日期
        user_id: Long, //用户的ID
        session_id: String, //Session的ID
        page_id: Long, //某个页面的ID
        action_time: String, //动作的时间点
        search_keyword: String, //用户搜索的关键词
        click_category_id: Long, //某一个商品品类的ID
        click_product_id: Long, //某一个商品的ID
        order_category_ids: String, //一次订单中所有品类的ID集合
        order_product_ids: String, //一次订单中所有商品的ID集合
        pay_category_ids: String, //一次支付中所有品类的ID集合
        pay_product_ids: String, //一次支付中所有商品的ID集合
        city_id: Long //城市 id
  )

}

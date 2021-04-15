package com.atguigu.spark.core.requirement

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * @author yml
  * 2021-03-19-11:31
  */
object Spark05_HotCategorySessionTop10 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Req")
    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.textFile("data/user_visit_action.txt")
    rdd.cache()

    val acc = new MyAcc
    sc.register(acc,"MyAcc")

    rdd.foreach(
      line => {
        val datas: Array[String] = line.split("_")
        if(datas(6) != "-1"){
          acc.add((datas(6),"click"))
        }else if(datas(8) != "null"){
          val s: Array[String] = datas(8).split(",")
          s.foreach(
            id => {
              acc.add((id,"order"))
            }
          )
        }else if(datas(10) != "null"){
          val s: Array[String] = datas(10).split(",")
          s.foreach(
            id => {
              acc.add((id,"pay"))
            }
          )
        }
      }
    )

    val categories: List[HotCategory] = acc.value.map(_._2).toList
    val result: List[HotCategory] = categories.sortWith(
      (hc1, hc2) => {
        if (hc1.clickcnt > hc2.clickcnt) {
          true
        } else if (hc1.clickcnt == hc2.clickcnt) {
          if (hc1.ordercnt > hc2.ordercnt) {
            true
          } else if (hc1.ordercnt == hc2.ordercnt) {
            hc1.paycnt > hc2.paycnt
          } else {
            false
          }
        } else {
          false
        }
      }
    ).take(10)

    //((品类,session),click)
    val top: RDD[String] = rdd.filter(
      line => {
        val datas: Array[String] = line.split("_")
        val s: List[String] = result.map(_.cid)
        s.contains(datas(6))
      }
    )

    val top1: RDD[((String, String), Int)] = top.flatMap(
      line => {
        val datas: Array[String] = line.split("_")
        List(((datas(6), datas(2)), 1))
      }
    ).reduceByKey(_ + _)  //((品类,session),sum)

    val top2: RDD[(String, (String, Int))] = top1.map {
      case ((a, b), c) => (a, (b, c))
    }

    val top3: RDD[(String, Iterable[(String, Int)])] = top2.groupByKey()

    val top4: RDD[(String, List[(String, Int)])] = top3.mapValues(
      t => {
        val list: List[(String, Int)] = t.toList
        list.sortBy(_._2)(Ordering.Int.reverse).take(10)
      }
    )
    //(品类,Iterator[(session,click)])

    top4.collect().foreach(println)

    sc.stop()

  }

  //样例类的参数会被编译为val的属性
  case class HotCategory(var cid:String,var clickcnt:Long,var ordercnt:Long,var paycnt:Long)

  class MyAcc extends AccumulatorV2[(String,String),mutable.Map[String,HotCategory]]{

    private val map = mutable.Map[String,HotCategory]()

    override def isZero: Boolean = map.isEmpty

    override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] = new MyAcc

    override def reset(): Unit = map.clear()

    override def add(v: (String, String)): Unit = {
      val (cid,ty) = v
      val hc: HotCategory = map.getOrElse(cid,HotCategory(cid,0L,0L,0L))
      ty match {
        case "click" => hc.clickcnt += 1
        case "order" => hc.ordercnt += 1
        case "pay" => hc.paycnt += 1
      }
      map.update(cid,hc)
    }

    override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotCategory]]): Unit = {
      val otherMap: mutable.Map[String, HotCategory] = other.value
      otherMap.foreach{
        case (cid,otherhc) => {
          val thishc: HotCategory = map.getOrElse(cid,HotCategory(cid,0L,0L,0L))
          thishc.clickcnt += otherhc.clickcnt
          thishc.ordercnt += otherhc.ordercnt
          thishc.paycnt += otherhc.paycnt

          map.update(cid,thishc)
        }
      }
    }

    override def value: mutable.Map[String, HotCategory] = map
  }

}

package com.atguigu.spark.core.requirement

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayOps

/**
  * @author yml
  * 2021-03-19-11:31
  */
object Spark03_HotCategoryTop10_Pro {

  def main(args: Array[String]): Unit = {

    //这种实现方式虽然数据量多，但是省去了3次reduceByKey，节省了3次shuffle阶段的IO，效率高

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Req")
    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.textFile("data/user_visit_action.txt")

    val result = rdd.flatMap(
      line => {
        val datas: ArrayOps.ofRef[String] = line.split("_")
        if(datas(6) != "-1"){
          List((datas(6),(1,0,0)))
        }else if(datas(8) != "null"){
          val order: Array[String] = datas(8).split(",")
          order.map(
            id => {
              (id,(0,1,0))
            }
          )
        }else if(datas(10) != "null"){
          val pay: Array[String] = datas(10).split(",")
          pay.map(
            id => {
              (id,(0,0,1))
            }
          )
        }else{
          Nil
        }
      }
    ).reduceByKey(
      (t1,t2) => {
        (t1._1 + t2._1,t1._2 + t2._2,t1._3 + t2._3)
      }
    ).sortBy(_._2,false).take(10)

    result.foreach(println(_))

    sc.stop()

  }

}

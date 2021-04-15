package com.atguigu.spark.core.acc

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * @author yml
  * 2021-03-17-16:26
  */
object Spark05_Var_BC {

  def main(args: Array[String]): Unit = {

    //TODO 累加器
    //一个Executor可以同时执行多个Task，取决于核数

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Acc")
    val sc = new SparkContext(conf)

    //笛卡尔积
    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("b",2),("c",3)),2)
    //val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a",4),("b",5),("c",6)),2)
    val map = mutable.Map(("a",1),("b",2),("c",3))

    //val rdd3: RDD[(String, (Int, Int))] = rdd1.join(rdd2)

    //避免了shuffle
    //闭包传输数据时，以Task为单位发送的
    //这里有2个分区 -> 2个Task -> 发送2份map
    //但是又有问题：一个Executor中有3个Task，所以有3份map，如果map很大，造成Executor内存不足，怎么办
    //所以不能跟随Task发送，要将map单独发送到Executor的内存中，并且只发送一份
    //但是RDD做不到，RDD只能发送Task结构
    //所以就需要广播变量（分布式共享只读变量）
    //广播变量可以将用户自定义数据发送给Executor的内存中，供该Executor的多个Task使用（只读）
    val bc: Broadcast[mutable.Map[String, Int]] = sc.broadcast(map)

    val rdd3: RDD[(String, (Int, Int))] = rdd1.map {
      case (k, v) => {
        (k, (v, bc.value.getOrElse(k, 0)))
      }
    }

    rdd3.collect().foreach(println)

    sc.stop()

  }

}

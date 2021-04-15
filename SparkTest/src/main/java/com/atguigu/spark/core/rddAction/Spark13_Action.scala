package com.atguigu.spark.core.rddAction

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author yml
  * 2021-03-16-17:02
  */
object Spark13_Action {

  def main(args: Array[String]): Unit = {

    //TODO RDD - 行动算子
    //行动算子：可以触发RDD执行的算子
    //Spark中，会将RDD按照分区数转换为Task，发送给Executor进行计算
    //Task包含了数据和算子
    //如果在算子中使用到了其他类，那么这个类需要可序列化
    //算子之外的操作是在driver中进行的，不涉及传输

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Transform")
    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.makeRDD(List("Hive","Hadoop","Spark"),2)

    val user = new User("H")

    //需要User可序列化
    //user.test(rdd).collect().foreach(println(_))
    //不要求User可序列化
    user.test1(rdd).collect().foreach(println(_))

    sc.stop()

  }

  class User(query : String) { //如果类中使用到了query，那么query会被编译为属性
    def test(rdd : RDD[String]) = {
      rdd.filter(_.contains(query))
    }

    def test1(rdd : RDD[String]) = {
      var q = query
      rdd.filter(_.contains(q))
    }
  }

}

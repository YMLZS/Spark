package com.atguigu.spark.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author yml
  * 2021-03-17-16:26
  */
object Spark02_Var_Acc {

  def main(args: Array[String]): Unit = {

    //TODO 累加器
    //封装自定义操作
    //Spark自带了累加器，可以直接使用

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Acc")
    val sc = new SparkContext(conf)


    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)

    //声明累加器
    val sum: LongAccumulator = sc.longAccumulator("sum")
    //double累加器
//    sc.doubleAccumulator()
    //集合累加器
//    sc.collectionAccumulator()

    //闭包，把sum传过去了
    rdd.foreach(
      num => sum.add(num)
    )

    //获取累加器的值
    println(sum.value)

    sc.stop()

  }

}

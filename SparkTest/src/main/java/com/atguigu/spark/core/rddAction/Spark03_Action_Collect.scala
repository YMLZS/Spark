package com.atguigu.spark.core.rddAction

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author yml
  * 2021-03-16-17:02
  */
object Spark03_Action_Collect {

  def main(args: Array[String]): Unit = {

    //TODO RDD - 行动算子


    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Transform")
    val sc = new SparkContext(conf)

    //collect：按照分区编号，顺序采集数据，以数组形式返回
    //collect将分布式数据汇总到driver中，可能会出现内存溢出，所以实际环境中一般不用collect
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)

    rdd.collect()

    sc.stop()

  }

}

package com.atguigu.spark.core.rddIO

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author yml
  * 2021-03-17-15:23
  */
object Spark02_Load {

  def main(args: Array[String]): Unit = {

    //TODO RDD - IO

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Persist")
    val sc = new SparkContext(conf)

    println(sc.textFile("output1").collect().mkString(","))
    println(sc.objectFile[(String,String)]("output2").collect().mkString(","))
    println(sc.sequenceFile[String,String]("output3").collect().mkString(","))

    sc.stop()

  }

}

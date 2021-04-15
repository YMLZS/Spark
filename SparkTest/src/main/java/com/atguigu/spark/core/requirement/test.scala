package com.atguigu.spark.core.requirement

import java.io

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * @author yml
  * 2021-03-19-11:31
  */
object test {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Req")
    val sc = new SparkContext(conf)

    val rdd: RDD[(String, List[(String, Int)])] = sc.makeRDD(List(("yml",List(("a",1),("b",1),("c",1)))))

    sc.stop()

  }

}

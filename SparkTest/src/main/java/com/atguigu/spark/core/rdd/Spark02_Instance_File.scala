package com.atguigu.spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author yml
  * 2021-03-12-9:55
  */
object Spark02_Instance_File {

  def main(args: Array[String]): Unit = {

    //TODO 从磁盘文件创建RDD

    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("Instance")
    val sc = new SparkContext(conf)

    //读取文件时，以行为单位进行读取
    //val rdd: RDD[String] = sc.textFile("data/word.txt")
    //文件路径可以是具体文件，也可以是文件夹，读取文件夹下的所有文件
    //val rdd: RDD[String] = sc.textFile("data")
    //也可以使用通配符，读取一些指定的文件
    //val rdd: RDD[String] = sc.textFile("data/word*.txt")
    //如果想要获取数据所在的文件相关属性，需要wholeTextFiles方法，返回的是对偶元组（键值对），K为文件路径，V为文件内容
    val rdd: RDD[(String, String)] = sc.wholeTextFiles("data")
    rdd.collect().foreach(println(_))

    sc.stop()

  }

}


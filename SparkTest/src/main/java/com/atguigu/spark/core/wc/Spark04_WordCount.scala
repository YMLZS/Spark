package com.atguigu.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author yml
  * 2021-03-10-10:07
  */
object Spark04_WordCount {

  def main(args: Array[String]): Unit = {

    //TODO Spark - WordCount案例 - 方式三
    //master : local => 1个线程
    //master : local[*] => 使用当前机器中最大的核数线程

    //1.建立Spark计算引擎的连接(环境)
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("WordCount")
    val sc = new SparkContext(conf)

    //2.数据的统计分析
    //各种功能组合在一起
    val lines: RDD[String] = sc.textFile("data/word.txt")
    val words: RDD[String] = lines.flatMap(_.split(" "))
    val tuple: RDD[(String, Int)] = words.map(word => (word,1))
    val wordCount: RDD[(String, Int)] = tuple.reduceByKey(_+_)
    //collect方法启动执行功能
    wordCount.collect().foreach(println(_))

    //3.关闭连接
    sc.stop()

  }

}

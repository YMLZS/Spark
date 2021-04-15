package com.atguigu.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author yml
  * 2021-03-10-10:07
  */
object Spark03_WordCount {

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
    //读取数据源，获取原始数据，得到一行一行的内容
    val lines: RDD[String] = sc.makeRDD(List(
      "hello hello hello"
    ))
    println("abc")
    //将数据进行切分，形成一个一个的单词
    val words: RDD[String] = lines.flatMap(
      t => {
        println("杨茂林")
        t.split(" ")
      }
    )
    val tuple: RDD[(String, Int)] = words.map(word => (word,1))
    //Spark提供了特殊的方法，对分组聚合的操作进行简化
    //reduceByKey的功能：数据处理中，如果有相同key，那么会对同一个key的value进行reduce操作
    //自动实现了对key分组，对value聚合
    //前提：数据是键值对
    val wordCount: RDD[(String, Int)] = tuple.reduceByKey(_+_)
    //打印结果
    wordCount.collect().foreach(println(_))
    wordCount.collect().foreach(println(_))
//    wordCount.cache()
//    wordCount.count()
//    wordCount.saveAsTextFile("yml3")

    //3.关闭连接
    sc.stop()

  }

}

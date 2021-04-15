package com.atguigu.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author yml
  * 2021-03-10-10:07
  */
object Spark01_WordCount {

  def main(args: Array[String]): Unit = {

    //TODO Spark - WordCount案例 - 方式一

    //1.建立Spark计算引擎的连接(环境)
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("WordCount")
    val sc = new SparkContext(conf)

    //2.数据的统计分析
    //读取数据源，获取原始数据，得到一行一行的内容
    val lines: RDD[String] = sc.textFile("data/word.txt")
    //将数据进行切分，形成一个一个的单词
    //val words: RDD[String] = lines.flatMap(_.split(" "))
    val words: RDD[String] = lines.flatMap(
      s => {
        println("杨茂林")
        s.split(" ")
      }
    )
    //根据单词进行分组
    val groups: RDD[(String, Iterable[String])] = words.groupBy(word=>word)
    //将分组后的数据进行分析
    val wordCount: RDD[(String, Int)] = groups.map {
      case (word, list) => {
        (word, list.size)
      }
    }
    //打印结果
    wordCount.collect().foreach(println(_))

    //3.关闭连接
    sc.stop()

  }

}

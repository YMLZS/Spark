package com.atguigu.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author yml
  * 2021-03-10-10:07
  */
object test {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("WordCount")
    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.makeRDD(List(
      "hello hello hello"
    ))

    val words: RDD[String] = lines.flatMap(
      t => {
        println("abc")
        t.split(" ")
      }
    )
    val tuple: RDD[(String, Int)] = words.map(word => (word,1))
    val wordCount: RDD[(String, Int)] = tuple.reduceByKey(_+_)

    tuple.collect().foreach(println(_))

    sc.stop()

  }

}

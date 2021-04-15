package com.atguigu.spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author yml
  * 2021-03-12-9:55
  */
object Spark02_Instance_File_Par {

  def main(args: Array[String]): Unit = {

    //TODO 从磁盘文件创建RDD

    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("Par")
    val sc = new SparkContext(conf)

    //第一个参数为文件路径，第二个参数为最小分区数量（不传使用默认值）
      //默认值math.min(defaultParallelism, 2)
      //defaultParallelism = local，为1
      //优先级：传递的分区参数 > 配置参数 > 默认值math.min(defaultParallelism, 2)

    //Spark中读取文件，分区数量是如何确定的？
    //1.Spark读取文件采用的是Hadoop的读取方式
    //2.底层实现
      //totalSize：文件总的字节数 7
      //goalSize：每个分区存放的字节数 7 / 2 = 3
      //7 / 3 = 2...1 但是1/3大于10%，所以是3个分区
    val rdd: RDD[String] = sc.textFile("data/word.txt",2)
    rdd.saveAsTextFile("output")

    sc.stop()

  }

}


package com.atguigu.spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author yml
  * 2021-03-12-9:55
  */
object Spark02_Instance_File_Data {

  def main(args: Array[String]): Unit = {

    //TODO 从磁盘文件创建RDD

    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("Instance")
    val sc = new SparkContext(conf)

    //totalSize：总的字节数 => 11
    //minPartitions：预期分区数量 => 2
    //goalSize：每个分区应该存放的字节数 => 11 / 2 = 5
    //分区数量：totalSize / goalSize = 11 / 5 = 2...1    1 / 5 > 10%   分区数为3

    //文件数据划分规则：
    //1.分区数的计算和数据的划分规则是两回事
      //计算分区数时，是按字节数计算的，但是存放数据不能按照字节数存放
    //2.Spark分区数据划分采用Hadoop的处理方式
      //读取数据时，是按行读取的，而非字节（如果按字节划分的话，可能会将一个完整的字拆开了）
    //3.Hadoop在读取数据时，不是按照数据的位置，而是按照数据的偏移量读取（从0开始）
    //4.Hadoop读取数据时，偏移量不会重复读取

    //word1.txt为7个字节，minPartitions = 2，所以应该是3个分区
    /*
      数据划分规则：
        1@@ => 0,1,2 （偏移量）
        2@@ => 3,4,5
        3   => 6

        0 => 7 / 2 => [0,3] 从偏移量0开始，读到3 => 1、2 （Hadoop按行读取）（偏移量）
        1 => 7 / 2 => [3,6] => 3
        2 => 7 / 2 => [6,7] => 空
     */
    /*
    yml lyc@@    0,1,2,3,4,5,6,7,8    [0,6]
    zds          9,10,11              [6,12]
     */
    val rdd: RDD[String] = sc.textFile("data/word2.txt",2)
    rdd.saveAsTextFile("output")

    sc.stop()

  }

}


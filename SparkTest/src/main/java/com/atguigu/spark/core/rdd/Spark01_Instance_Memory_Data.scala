package com.atguigu.spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author yml
  * 2021-03-12-9:55
  */
object Spark01_Instance_Memory_Data {

  def main(args: Array[String]): Unit = {

    //TODO 从内存创建RDD

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Instance")
    val sc = new SparkContext(conf)

    //数据是怎么分配到每个分区的？
    //[1,2,3,4,5]
    //2个分区：[1,2] [3,4,5]
    //3个分区：[1] [2,3] [4,5]
    /*
      [1,2,3,4,5,6,7]的分区过程
        1、numSlices = 3 所以分区号为0,1,2
        2、start = (i * length / numSlices).toInt    end = ((i+1) * length / numSlices).toInt
        3、得出0 => [0,2) => [1,2]
              1 => [2,4) => [3,4]
              2 => [4,7) => [5,6,7]
     */
    val seq = Seq(1,2,3,4,5,6,7)
    val rdd: RDD[Int] = sc.makeRDD(seq,3)
    rdd.saveAsTextFile("output")

    sc.stop()


  }

}


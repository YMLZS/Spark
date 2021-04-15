package com.atguigu.spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author yml
  * 2021-03-12-9:55
  */
object Spark01_Instance_Memory_Par {

  def main(args: Array[String]): Unit = {

    //TODO 从内存创建RDD

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Instance")
    val sc = new SparkContext(conf)

    val seq = Seq(1,2,3,4)
    //makeRDD有两个参数，第一个表示需要处理的数据集，第二个表示分区的数量（默认值为local对应的线程数）
    //底层实现（Local）：scheduler.conf.getInt("spark.default.parallelism", totalCores)
      //totalCores为当前环境中最大的逻辑核数
    //底层实现（集群）：conf.getInt("spark.default.parallelism", math.max(totalCoreCount.get(), 2))
      //默认从SparkConf中获取配置参数，与2作比较，取最大值，集群模式最少是2个分区

    //优先级：传递的分区参数 > 配置参数 > 默认值
    val rdd: RDD[Int] = sc.makeRDD(seq,2)
    //将数据的计算结果保存为分区文件（一个分区一个文件）
    rdd.saveAsTextFile("output")

    sc.stop()


  }

}


package com.atguigu.spark.core.rddPersist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author yml
  * 2021-03-17-15:23
  */
object Spark02_CheckPoint {

  def main(args: Array[String]): Unit = {

    //TODO RDD - 检查点
    //在开发中如果要重复使用同一个rdd，需要将这个rdd的结果保存下来
    //检查点可以真正持久化rdd，供其他程序使用
    //如果单独调用checkPoint会导致job执行多次，所以一般和cache连用，checkPoint会从cache中读取数据

    //缓存和检查点的区别
    //1.cache不会切断血缘关系，checkPoint会切断血缘
    //2.cache中的数据一般保存在内存或磁盘，可靠性低，checkPoint一般保存在HDFS上，可靠性高

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Persist")
    val sc = new SparkContext(conf)
    //需要设置检查点保存路径，最好保存在HDFS上
    sc.setCheckpointDir("cp")

    val rdd: RDD[String] = sc.makeRDD(List("Hello Spark"))
    val rdd1: RDD[String] = rdd.flatMap(_.split(" "))
    val rdd2: RDD[(String, Int)] = rdd1.map((_,1))

    //我们应该将rdd2的结果保存下来，再进行别的操作
    rdd2.cache()
    rdd2.checkpoint()

    val rdd3: RDD[(String, Int)] = rdd2.reduceByKey(_+_)
    rdd3.collect().foreach(println(_))

    val rdd4: RDD[(String, Iterable[Int])] = rdd2.groupByKey()
    rdd4.collect().foreach(println(_))

    sc.stop()

  }

}

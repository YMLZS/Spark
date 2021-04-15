package com.atguigu.spark.core.rddPersist

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author yml
  * 2021-03-17-15:23
  */
object Spark01_Persist {

  def main(args: Array[String]): Unit = {

    //TODO RDD - 持久化
    //在开发中如果要重复使用同一个rdd，需要将这个rdd的结果保存下来
    //1.保存在cache缓存中(计算节点的内存中)
    //2.保存在文件中persist
    //这两种持久化方法在程序执行完后，都会失效（伪持久化）

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Persist")
    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.makeRDD(List("Hello Spark"))
    val rdd1: RDD[String] = rdd.flatMap(_.split(" "))
    val rdd2: RDD[(String, Int)] = rdd1.map((_,1))

    //这样做其实还是整条路执行了两遍
//    val rdd3: RDD[(String, Int)] = rdd2.reduceByKey(_+_)
//    rdd3.collect().foreach(println(_))
//
//    val rdd4: RDD[(String, Iterable[Int])] = rdd2.groupByKey()
//    rdd4.collect().foreach(println(_))

    //我们应该将rdd2的结果保存下来，再进行别的操作
    rdd2.cache() //底层其实就是rdd2.persist(StorageLevel.MEMORY_ONLY)
    rdd2.persist(StorageLevel.MEMORY_AND_DISK) //同时保存在内存和磁盘

    val rdd3: RDD[(String, Int)] = rdd2.reduceByKey(_+_)
    rdd3.collect().foreach(println(_))

    val rdd4: RDD[(String, Iterable[Int])] = rdd2.groupByKey()
    rdd4.collect().foreach(println(_))

    sc.stop()

  }

}

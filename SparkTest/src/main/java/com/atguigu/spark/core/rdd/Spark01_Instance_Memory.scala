package com.atguigu.spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author yml
  * 2021-03-12-9:55
  */
object Spark01_Instance_Memory {

  def main(args: Array[String]): Unit = {

    //TODO 从内存创建RDD

    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("Instance")
    val sc = new SparkContext(conf)

    //Seq或者Seq的混入类（List）
    val seq = Seq(1,2,3,4)
    //val rdd: RDD[Int] = sc.parallelize(seq) //parallelize：并行
    val rdd: RDD[Int] = sc.makeRDD(seq) //makeRDD与parallelize完全一样,makeRDD底层调用了parallelize
    rdd.collect().foreach(println(_))

    sc.stop()


  }

}


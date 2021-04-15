package com.atguigu.spark.core.rddTransform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author yml
  * 2021-03-12-15:36
  */
object Spark09_Transform_Distinct {

  def main(args: Array[String]): Unit = {

    //TODO RDD转换算子

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Transform")
    val sc = new SparkContext(conf)

    //distinct：去重
    //在Scala的集合中，distinct仅表示去重
    //在Spark中，distinct表示全局去重，有shuffle机制，将所有数据全部打乱进行去重
    //底层：map(x => (x, null)).reduceByKey((x, _) => x, numPartitions).map(_._1)
    //底层的reduceByKey存在shuffle机制
    //可以设定去重完的分区数量
    val rdd: RDD[Int] = sc.makeRDD(List(1,1,2,2,3,3,4,4))
    val rdd1: RDD[Int] = rdd.distinct(2)
    rdd1.saveAsTextFile("output")
    //rdd1.collect().foreach(println(_))

    sc.stop()

  }

}

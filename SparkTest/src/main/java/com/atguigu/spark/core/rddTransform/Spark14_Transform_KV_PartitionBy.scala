package com.atguigu.spark.core.rddTransform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * @author yml
  * 2021-03-12-15:36
  */
object Spark14_Transform_KV_PartitionBy {

  def main(args: Array[String]): Unit = {

    //TODO RDD转换算子

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Transform")
    val sc = new SparkContext(conf)

    //partitionBy：根据指定的规则，对数据进行重新分区（强调数据分区的变化，之前都是强调分区数量的变化）
    //必须是k-v泛型的RDD才有partitionBy方法

    //思考：为什么都是RDD，rdd没有partitionBy，而rdd1有partitionBy呢？
    //说明RDD没有partitionBy方法，原理在于隐式转换（二次编译），将键值对类型的RDD隐式转换为PairRDDFunctions
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)
    val rdd1: RDD[(Int, Int)] = rdd.map((_,1))
    //需要传递分区器对象
    //Spark提供了3个分区器，其中一个为HashPartitioner，哈希值与分区数取余
    //Spark的默认分区器为HashPartitioner
    val rdd2: RDD[(Int, Int)] = rdd1.partitionBy(new HashPartitioner(2))

    sc.stop()

  }

}

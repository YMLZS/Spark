package com.atguigu.spark.core.rddTransform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * @author yml
  * 2021-03-12-15:36
  */
object Spark15_Transform_KV_ReduceByKey {

  def main(args: Array[String]): Unit = {

    //TODO RDD转换算子

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Transform")
    val sc = new SparkContext(conf)

    //reduceByKey：将相同key的数据的value聚合在一起（分组聚合）
    //特点：分组内和分组间的计算逻辑相同
    //一定有shuffle。第二个参数可以指定分区数量
    //shuffle一定会落盘，但是如果落盘的数据量少，那么性能会提升很多
    //在shuffle落盘前，对数据提前进行聚合，称为预聚合（Combiner），也就是分区内聚合
    //reduceByKey存在分区内计算（预聚合）和分区间计算（shuffle）
    //所以reduceByKey的效率高，在分组聚合时，优先使用reduceByKey
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("a",2),("a",3)),2)
    val rdd1: RDD[(String, Int)] = rdd.reduceByKey(_+_)
    rdd1.saveAsTextFile("output3")
    //rdd1.collect().foreach(println(_))

    sc.stop()

  }

}

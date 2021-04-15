package com.atguigu.spark.core.rddTransform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author yml
  * 2021-03-12-15:36
  */
object Spark21_Transform_KV_OuterJoin {

  def main(args: Array[String]): Unit = {

    //TODO RDD转换算子

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Transform")
    val sc = new SparkContext(conf)


    val rdd = sc.makeRDD(
      List(("a",1),("b",2),("c",3)))
    val rdd1 = sc.makeRDD(
      List(("a",4),("d",5),("c",6),("a",2)))

    //leftOuterJoin：两个数据集中如果有相同的key，对他们的value进行连接
    //左表的全部数据一定会得到，右表只有匹配上的才会得到
    val rdd2: RDD[(String, (Int, Option[Int]))] = rdd.leftOuterJoin(rdd1)
    //rightOuterJoin：两个数据集中如果有相同的key，对他们的value进行连接
    //右表的全部数据一定会得到，左表只有匹配上的才会得到
    val rdd3: RDD[(String, (Option[Int], Int))] = rdd.rightOuterJoin(rdd1)
    //满外连接，全部保留，相同key的进行分组
    val rdd4: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd.cogroup(rdd1)

    //rdd2.collect().foreach(println(_))
    rdd4.collect().foreach(println(_))

    sc.stop()

  }

}

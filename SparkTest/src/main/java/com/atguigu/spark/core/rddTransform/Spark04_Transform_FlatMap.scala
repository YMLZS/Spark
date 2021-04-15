package com.atguigu.spark.core.rddTransform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author yml
  * 2021-03-12-15:36
  */
object Spark04_Transform_FlatMap {

  def main(args: Array[String]): Unit = {

    //TODO RDD转换算子

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Transform")
    val sc = new SparkContext(conf)

    //flatMap：扁平化，将一个整体拆分成一个一个的个体
    val rdd: RDD[List[Int]] = sc.makeRDD(
      List(
        List(1, 2), List(3, 4)
      )
    )
    //输入：数据集中的元素类型 => 输出：扁平化后的数据容器
    val newRdd: RDD[Int] = rdd.flatMap(list => list)
    //newRdd.collect().foreach(println(_))

    //分词
    val rdd1: RDD[String] = sc.makeRDD(
      List("Hello World", "Hello Spark")
    )
    rdd1.flatMap(
      str => {
        str.split(" ") //切分完后为数组（容器），可直接返回
      }
    )

    //模式匹配
    val rdd2: RDD[Any] = sc.makeRDD(
      List(
        List(1, 2), 3, List(4, 5)
      )
    )
    val newRdd2: RDD[Any] = rdd2.flatMap {
      case list: List[_] => list
      case num: Int => List(num)
    }
    newRdd2.collect().foreach(println(_))

    sc.stop()

  }

}

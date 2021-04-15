package com.atguigu.spark.core.rddTransform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author yml
  * 2021-03-12-15:36
  */
object Spark06_Transform_GroupBy {

  def main(args: Array[String]): Unit = {

    //TODO RDD转换算子

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Transform")
    val sc = new SparkContext(conf)

    //groupBy
    //Int（元素） => 规则 => K（组名）
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)
    val rdd1 = rdd.groupBy(
      num => {
//        if (num % 2 == 0) {
//          "偶数"
//        } else {
//          "奇数"
//        }
        //如果不关心组名
        num % 2
      }
    )
    rdd1.collect().foreach(println(_))

    //shuffle打乱全部数据后，groupBy分组
    //1.groupBy会改变分区数据，分组后，一个组的数据在一个分区中，但一个分区可能会有多个组
    //2.可能导致分区间的数据不均衡
    //3.默认情况下，分组前和分组后的分区数量不变
    //4.groupBy会在执行过程中进行等待，直到所有的数据分组完成后，才能往后执行
      //这个等待过程是通过磁盘完成的，所以shuffle操作是一定要落盘的
    //5.所有的shuffle操作，都有能力改变分区
      //如果一个算子能改变分区，90%都有shuffle
    val rdd2: RDD[String] = sc.makeRDD(List("Hive","Spark","Hadoop","Scala"),3)
    //rdd2.saveAsTextFile("output")
    val rdd3 = rdd2.groupBy(_.substring(0,1)) //可以通过参数改变分区数
    rdd3.saveAsTextFile("output")

    sc.stop()

  }

}

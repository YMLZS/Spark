package com.atguigu.spark.core.rddTransform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author yml
  * 2021-03-12-15:36
  */
object Spark08_Transform_Sample {

  def main(args: Array[String]): Unit = {

    //TODO RDD转换算子

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Transform")
    val sc = new SparkContext(conf)

    //sample：在数据集中随机抽取数据
    //1.抽取数据后放回，可能会被第二次抽取到（第一个参数为true）
    //2.抽取数据后不放回，不会被第二次取到（第一个参数为false）
    //第一个参数为false时，第二个参数表示每个数据被抽取的几率[0,1]
    //第一个参数为true时，第二个参数表示每个数据预期被抽取几次，数值大于1
    //第三个参数为随机数种子，如果固定随机数种子（默认不传，种子是随机的），那么每次抽取的数据都相同
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6,7,8,9,10))
    val rdd1: RDD[Int] = rdd.sample(true,2,1)
    rdd1.collect().foreach(println(_))

    sc.stop()

  }

}

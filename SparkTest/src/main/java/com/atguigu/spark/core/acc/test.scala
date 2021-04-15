package com.atguigu.spark.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

/**
  * @author yml
  * 2021-03-24-21:59
  */
object test {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
    val sc = new SparkContext(conf)

    val rdd1 = sc.makeRDD(List((2, 1), (1, 1)),2)
    val rdd2 = sc.makeRDD(List((2, 1), (1, 1)),2)

//    val rdd3: RDD[(Int, Iterable[Int])] = rdd1.groupByKey()
//    val rdd4: RDD[(Int, Iterable[Int])] = rdd2.groupByKey()


    val rdd5= rdd1.join(rdd2,3)
    println(rdd5.dependencies)
    println(rdd5.toDebugString)

    rdd5.collect()


    Thread.sleep(1000000)
  }

  class MyPartitioner extends Partitioner {
    override def numPartitions: Int = 5

    override def getPartition(key: Any): Int = {
      key match {
        case 1 => 0
        case 2 => 1
      }
    }
  }

}

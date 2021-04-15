package com.atguigu.spark.core.rddAction

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author yml
  * 2021-03-16-17:02
  */
object Spark12_Action {

  def main(args: Array[String]): Unit = {

    //TODO RDD - 行动算子
    //行动算子：可以触发RDD执行的算子
    //Spark中，会将RDD按照分区数转换为Task，发送给Executor进行计算
    //Task包含了数据和算子
    //如果在算子中使用到了其他类，那么这个类需要可序列化
    //算子之外的操作是在driver中进行的，不涉及传输

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Transform")
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)

    val user = new User

    //闭包
    //Spark作业执行之前，会判断我们的数据准备中，是否存在闭包，检测闭包中的对象是否可序列化，称为闭包检测
    rdd.foreach(
      num => {
        println(user.age + num)
      }
    )

    sc.stop()

  }

  class User extends Serializable {
    var age : Int = 30
  }

}

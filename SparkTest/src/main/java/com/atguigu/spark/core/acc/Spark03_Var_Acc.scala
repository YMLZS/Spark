package com.atguigu.spark.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * @author yml
  * 2021-03-17-16:26
  */
object Spark03_Var_Acc {

  def main(args: Array[String]): Unit = {

    //TODO 累加器
    //封装自定义操作
    //Spark自带了累加器，可以直接使用

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Acc")
    val sc = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(
      List(("Hello", 1), ("Hello", 2), ("Hello", 3)), 2
    )

    //1.声明累加器
    val acc = new WordCountAcc
    //2.注册累加器，让Spark知道
    sc.register(acc,"WordCountAcc")

    rdd.foreach(
      t => {
        //3.使用累计器
        acc.add(t)
      }
    )

    //4.获取累加器的结果
    println(acc.value)

    sc.stop()

  }

  //自定义WordCount累加器
  //1.继承AccumulatorV2
  //2.确定泛型：
    //IN: 向累加器中累加的值类型
    //OUT: 累加器的结果类型
  class WordCountAcc extends AccumulatorV2[(String,Int),mutable.Map[String,Int]]{
    private val map = mutable.Map[String,Int]()

    //判断累加器是否为初始状态（序列化时调用）
    override def isZero: Boolean = {
      println("判断累加器是否为初始状态")
      map.isEmpty
    }

    //复制累加器（序列化时调用）
    override def copy(): AccumulatorV2[(String, Int), mutable.Map[String, Int]] = {
      println("复制累加器")
      new WordCountAcc
    }

    //重置累加器（序列化时调用）
    override def reset(): Unit = {
      println("重置累加器")
      map.clear()
    }

    //将数据向累加器中添加
    override def add(v: (String, Int)): Unit = {
      println("开始添加")
      val i: Int = map.getOrElse(v._1,0)
      map.update(v._1,i + v._2)
    }

    //合并累加器
    override def merge(other: AccumulatorV2[(String, Int), mutable.Map[String, Int]]): Unit = {
      println("开始合并")
      val otherMap: mutable.Map[String, Int] = other.value
      otherMap.foreach{
        case (word,num) => {
            val i: Int = map.getOrElse(word,0)
            map.update(word,i + num)
        }
      }
    }

    //获取累加器的结果
    override def value: mutable.Map[String, Int] = {
      map
    }
  }

}

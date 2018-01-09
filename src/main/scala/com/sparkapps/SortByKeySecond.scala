package com.sparkapps

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by analysis2 on 2016/11/18.
  */
object SortByKeySecond {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SortByKeySecond")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val one = Array(5,4,10,4)
    val two = Array(15,25,13,1)
    val data = sc.parallelize(one.zip(two))
    data.map{
      case (first,second)=>(SortSecondKey(first,second),first+","+second)
    }.sortByKey().foreach(a=>println(a._2))
    sc.stop()
  }
}

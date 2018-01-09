package com.za.apps

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount")
      .setMaster("local")
    //.setMaster("spark://master:7077")
    val sc = new SparkContext(conf)
    val word_text = sc.textFile("file:///D:\\服务器管理\\neo4j-enterprise-3.2.1-unix\\neo4j-enterprise-3.2.1\\NOTICE.txt")
    val word = word_text.flatMap(_.split(" ")).map((_,1))
    val orderWordcount = word.reduceByKey(_+_).sortBy(_._2,false)
    orderWordcount.take(10).foreach(println)
    sc.stop()
  }
}

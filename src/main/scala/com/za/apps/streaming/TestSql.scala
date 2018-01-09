package com.za.apps.streaming

import kafka.utils.Json
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 天眼查数据清洗，从kafka中将数据清洗之后，存放在hdfs中，以parquet的格式存储
  */
object TestSql {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TestSql")
                             .setMaster("local[2]")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val data = sqlContext.read.load("hdfs://master:9000/user/data/company_item")

    data.show(10)

    println("================:"+data.count());
  }
}

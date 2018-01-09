package com.za.apps.sql

import java.util

import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable

/**
  * Created by analysis2 on 2016/9/12.
  */
object SparkSqlTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkSqlTest")
      .setMaster("local")
    //.setMaster("spark://master:7077")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    val groups = sqlContext.read.json("file:///D://test_data/group.json")
    val ips = sqlContext.read.json("file:///D://test_data/ip.json")
    groups.show()
    ips.show()

    import org.apache.spark.sql.functions._

    val match_groups = udf((ip:String,ips:mutable.WrappedArray[String])=>{
      val now = ip.split('.')(3).toInt
      var flag =false
      ips.foreach(i=>{
          val start = i.split("-")(0).split('.')(3).toInt
          val end = i.split("-")(1).split('.')(3).toInt
          if(now>=start && now<=end){
            flag=true
          }
      })
      flag
    })

    ips.join(groups,match_groups(ips.col("ip"),groups.col("ips"))).show()


  }
}

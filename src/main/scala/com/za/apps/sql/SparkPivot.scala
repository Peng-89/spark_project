package com.za.apps.sql

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}

object SparkPivot {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkPivot")
      .setMaster("local")
    //.setMaster("spark://master:7077")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    //val data = sc.textFile("file:////platform/ml-1m/ratings.dat",10)
    val data = sc.textFile("file:///D:\\迅雷下载\\ml-1m (1)\\ml-1m\\ratings.dat",10)
    val cc = data.map(a=>Row.fromSeq(a.split("::").toSeq))

    val schema = StructType(
      StructField("user",StringType)::
        StructField("movie",StringType)::
        StructField("rating",StringType)::
        StructField("c",StringType)::Nil
    )

    val df = sqlContext.createDataFrame(cc,schema)
    val popular=   df.groupBy("movie").count().sort('count.desc).map(_.getAs[String]("movie")).take(20).toSeq

    import org.apache.spark.sql.functions._
    val dd =df.groupBy("user").pivot("movie",popular).agg(coalesce(first("rating"),lit(3)))
    dd.show

    df.registerTempTable("ratings")

    sqlContext.sql("select count(0) from ratings").show
  }
}

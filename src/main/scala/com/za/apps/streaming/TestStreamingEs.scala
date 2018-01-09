package com.za.apps.streaming

import kafka.serializer.StringDecoder
import kafka.utils.Json
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.apache.spark.streaming.kafka.KafkaManager
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

/**
  * 天眼查数据清洗，从kafka中将数据清洗之后，存放在hdfs中，以parquet的格式存储
  */
object TestStreamingEs {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TestStreamingEs")
                             //.setMaster("local[2]")
                             .set("spark.es.nodes","10.18.67.111")
                             .set("spark.es.port","9200")
                             .setMaster("spark://master:7077")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(10))
    val sqlContext = new SQLContext(sc)

    val line = ssc.socketTextStream("master",9999)


    line.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        //包括行业的item
        val company_item = rdd.map(Json.parseFull)
          .filter(_ match {
            case None => false
            case Some(_) => true
          }).map(data => {
             val result = data.get.asInstanceOf[Map[String, Object]]
             Row(result.getOrElse("city","0"),
                 result.get("score").get.toString.toInt,
                 result.getOrElse("industry","0"),
                 result.getOrElse("companyName","0"),
                 result.getOrElse("province","0"),
                 result.getOrElse("item",0l),
                 result.getOrElse("crateTime","0"),
                 result.getOrElse("regStatus","0")
             )
        })
        val schema =
           StructType(
                StructField("city", StringType, true)::
                StructField("score", IntegerType, true)::
                StructField("industry", StringType, true)::
                StructField("companyName", StringType, true)::
                StructField("province", StringType, true)::
                StructField("item", StringType, true)::
                StructField("crateTime", StringType, true)::
                StructField("regStatus", StringType, true)::
                Nil)
         val company_item_df =sqlContext.createDataFrame(company_item,schema)
        company_item_df.show()
         //实时更新到ES中
         company_item_df.write.format("es").mode(SaveMode.Append).save("test/item")
       }
    })

    ssc.start()
    ssc.awaitTermination()
    //关闭StreamingContext
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = {
             ssc.stop()
      }
    })


  }
}

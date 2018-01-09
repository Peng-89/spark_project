package com.za.apps.streaming

import kafka.serializer.StringDecoder
import kafka.utils.Json
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types._
import org.apache.spark.streaming.kafka.{KafkaManager, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

import scala.collection.Iterable
import scala.collection.immutable.HashMap

/**
  * 天眼查数据清洗，从kafka中将数据清洗之后，存放在hdfs中，以parquet的格式存储
  */
object TycDataCleaning {
  /**
    * 将list转成tuple的方法
    * @param as
    * @tparam A
    * @return
    */
  def toTuple[A <: Object](as:List[A]):Product = {
    val tupleClass = Class.forName("scala.Tuple" + as.size)
    tupleClass.getConstructors.apply(0).newInstance(as:_*).asInstanceOf[Product]
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TycDataCleaning")
                             //.setMaster("local")
                             .set("spark.es.nodes","10.18.67.111")
                             .set("spark.es.port","9200")
                             .setMaster("spark://master:7077")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(600))
    val sqlContext = new SQLContext(sc)

    //配置kafka的参数
    val kafkaParms = Map("metadata.broker.list"->"master:9092,slave1:9092,slave2:9092",
                          "group.id"->"tyc-spark-company",
                          "auto.offset.reset"->"smallest")
    //设置topic
    val topics=Set("task_crawler_tyc_details_get")
    //构造定义的KafkaManager
    val kafkaManager  = new KafkaManager(kafkaParms)
    //通过createDirectStream构造输入流，这种方式将kafka看成底层的文件系统
    val streams  = kafkaManager.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParms,topics)
    import sqlContext.implicits._
    import org.apache.spark.sql.SaveMode
    import org.elasticsearch.spark._
    import org.elasticsearch.spark.sql._
    streams.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        val item = rdd.filter(!_._2.contains("regCapital")).map(_._2)
        //包括行业的item
        val company_item = item.map(Json.parseFull)
          .filter(_ match {
            case None => false
            case Some(_) => true
          }).filter(data=> {
             //通过redis中保存的item来过滤已经存在的数据
             //获取redis客户端
             val jedis = new Jedis("10.18.67.111",6379)
             //设置密码
             jedis.auth("redispass")
             val item = data.getOrElse("item",0l)
             var flag =true
             if(!jedis.hexists("company_item",item.toString)){
               //更新item_set数据
               jedis.hset("company_item",item.toString,item.toString)
             }else{
               flag=false
             }
             jedis.close()
             flag
        }).map(data => {
             val result = data.get.asInstanceOf[Map[String, Object]]
             Row(result.getOrElse("city","0"),
                 result.getOrElse("score","0"),
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
                StructField("score", StringType, true)::
                StructField("industry", StringType, true)::
                StructField("companyName", StringType, true)::
                StructField("province", StringType, true)::
                StructField("item", StringType, true)::
                StructField("crateTime", StringType, true)::
                StructField("regStatus", StringType, true)::
                Nil)
         val company_item_df =sqlContext.createDataFrame(company_item,schema)
         company_item_df.cache()
         company_item_df.save("hdfs://master:9000/hive/company_parquet",
                                                   "parquet",
                                                    SaveMode.Append)
         //实时更新到ES中
         company_item_df.write.format("es").mode(SaveMode.Append).save("company_item/item")

         kafkaManager.updateZKOffsets(rdd)
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

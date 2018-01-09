package com.za.apps.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SaveMode}
import redis.clients.jedis.Jedis

/**
  * Created by analysis2 on 2016/9/12.
  */
object SparkCompany {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkCompany")
      .setMaster("local")
      .set("spark.es.nodes","10.18.67.111")
      .set("spark.es.port","9200")
    //.setMaster("spark://master:7077")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val company_json = sqlContext.read.format("json").load("/hive/company_all.json").select("city", "companyName",
                   "crateTime", "industry", "item", "province", "regStatus", "score")

    company_json.cache()

    company_json.count()

    val company_result = company_json.map(rdd => {
      (rdd.getAs[String]("item"), rdd)
    })

    val result = company_result.reduceByKey((x, y) => y).map(_._2)

    result.cache()

    result.count()

    val dd = sqlContext.createDataFrame(result, company_json.schema)

    dd.cache()

    dd.count

    import org.elasticsearch.spark._
    import org.elasticsearch.spark.sql._
    dd.write.format("es").mode(SaveMode.Append).save("company_item/item")


    dd.select("item").rdd.foreachPartition(partion => {
      //获取redis客户端
      val jedis = new Jedis("10.18.67.111", 6379)
      //设置密码
      jedis.auth("redispass")
      partion.foreach(row => {
        val item = row.getAs[String]("item")
        jedis.hset("company_item", item, item)
      })
      jedis.close()
    })

    val company_json2 = sqlContext.read.load("/hive/company_parquet")
    company_json2.cache()
    company_json2.count()
    val result2 = company_json2.rdd.filter(data=> {
      //通过redis中保存的item来过滤已经存在的数据
      //获取redis客户端
      val jedis = new Jedis("10.18.67.111",6379)
      //设置密码
      jedis.auth("redispass")
      val item = data.getAs[String]("item")
      var flag =true
      if(!jedis.hexists("company_item",item)){
        //更新item_set数据
        jedis.hset("company_item",item,item)
      }else{
        flag=false
      }
      jedis.close()
      flag
    })
    val nn= sqlContext.createDataFrame(result2, company_json2.schema)
    nn.write.format("es").mode(SaveMode.Append).save("company_item/item")

  }
}

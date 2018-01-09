package com.za.apps.sql

import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by analysis2 on 2016/10/25.
  */
object SparkEsJoinDetail {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkEsJoinDetail")
      //.set("spark.es.nodes", "10.18.67.111")
      //.set("spark.es.port", "9200")
      .setMaster("spark://master:7077")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val data = sqlContext.read.format("es").load("company_item/item").select("item","companyName").withColumnRenamed("item","item_l")

    //去重
    val company_result = data.map(rdd => {
      (rdd.getAs[String]("item_l"), rdd)
    })

    val result = company_result.reduceByKey((x, y) => y).map(_._2)


    val resultdistinct = sqlContext.createDataFrame(result, data.schema)


    val detail = sqlContext.read.format("es").load("company_detail/detail").distinct()


    val joindata = detail.join(resultdistinct,detail.col("item").equalTo(resultdistinct.col("item_l")),"inner")

    joindata.repartition(100).write.mode(SaveMode.Overwrite).save("hdfs://master:9000/user/data/company_item_detail")
  }
}

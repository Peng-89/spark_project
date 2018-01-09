package com.za.apps.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.hive.HiveContext

/**
  *
  spark-shell --master spark://master:7077 \
            --conf "spark.mongodb.input.uri=mongodb://master/zn.company_details?readPreference=primaryPreferred" \
            --conf "spark.mongodb.output.uri=mongodb://master/zn.company_details" \
			--executor-memory 10g --executor-cores 10 --total-executor-cores 30 \
            --jars /platform/spark/spark-1.6.2/lib/mongo-spark-connector_2.10-1.1.0-assembly.jar,/platform/spark/spark-1.6.2/lib/elasticsearch-spark_2.10-2.3.4.jar

  */
object SparkEsSort {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkEsSort")
      //.set("spark.es.nodes", "10.18.67.111")
      //.set("spark.es.port", "9200")
      .setMaster("spark://master:7077")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._

    val data = sqlContext.read.format("es").load("company_item/item")

    //去重
    val company_result = data.map(rdd => {
      (rdd.getAs[String]("item"), rdd)
    })

    val result = company_result.reduceByKey((x, y) => y).map(_._2)


    val resultdistinct = sqlContext.createDataFrame(result, data.schema)


    val detail = sqlContext.read.format("es").load("company_detail/detail").distinct().select("item").withColumnRenamed("item","item_detail")


    val joindata = resultdistinct.join(detail,resultdistinct.col("item").equalTo(detail.col("item_detail")),"left_outer")

    val lefdata = joindata.where("item_detail is null")

    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.expressions.Window

    val data1 = lefdata.withColumn("item_long",lefdata.col("item").cast("long"))
    val w = Window.orderBy(data1.col("item_long").asc)
    val sort=data1.withColumn("pxh",row_number over(w))

//    sort.map(a=>a match {
//      case Row(a,b,c) =>(a,b,c)
//    })
    //val tmp = sort.groupBy("item").count
    //tmp.sort(tmp.col("count").desc).show

    //data.sort(data.col("pxh").desc) 排序
    import com.mongodb.spark.sql._
    import com.mongodb.spark.config._

    val readConfig = ReadConfig(Map("collection" -> "company_sort", "readPreference.name" -> "secondaryPreferred"), Some(ReadConfig(sc)))
    //只同步300W
    sort.limit(3000000).write.format("com.mongodb.spark.sql").mode(SaveMode.Overwrite).options(readConfig.asOptions).save()
  }
}

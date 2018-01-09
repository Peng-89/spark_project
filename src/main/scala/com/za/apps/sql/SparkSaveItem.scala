package com.za.apps.sql

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  spark-shell --master spark://master:7077 \
            --conf "spark.mongodb.input.uri=mongodb://master/zn.company_details?readPreference=primaryPreferred" \
            --conf "spark.mongodb.output.uri=mongodb://master/zn.company_details" \
			--executor-memory 10g --executor-cores 10 --total-executor-cores 30 \
            --jars /platform/spark/spark-1.6.2/lib/mongo-spark-connector_2.10-1.1.0-assembly.jar,/platform/spark/spark-1.6.2/lib/elasticsearch-spark_2.10-2.3.4.jar

  */
object SparkSaveItem {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkSaveItem")
      //.set("spark.es.nodes", "10.18.67.111")
      //.set("spark.es.port", "9200")
      .setMaster("spark://master:7077")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)

    val data = sqlContext.read.format("es").load("company_item/item")

    //去重
    val company_result = data.map(rdd => {
      (rdd.getAs[String]("item"), rdd)
    })

    val result = company_result.reduceByKey((x, y) => y).map(_._2)


    val resultdistinct = sqlContext.createDataFrame(result, data.schema)

    resultdistinct.repartition(100).write.
      mode(SaveMode.Overwrite).save("hdfs://master:9000/user/data/company_item")

    sc.stop()
  }
}

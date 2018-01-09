package com.za.apps.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by analysis2 on 2016/8/25.
  */
object SparkDatacleaning {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkDatacleaning").setMaster("spark://master:7077")
                              .set("spark.es.nodes","10.18.67.111")
                              .set("spark.es.port","9200")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    import scala.collection.mutable
    //加载json数据
    val data = sqlContext.read.format("json").load("hdfs://master:9000/hive/company_item_useable")
    //缓存数据
    data.cache()
    data.count()
    //注册成表
    //data.registerTempTable("company")
    //是用dataFrame来处理
    val result = data.select("content.data.nodes.id", "content.data.nodes.properties.name", "content.data.nodes.labels")
    //val result = sqlContext.sql("select content.data.nodes.id,content.data.nodes.properties.name,content.data.nodes.labels from company ")

    //分别获取构造ID的df，name的df，label的df进行join，使用df的join可以使用spark的高级特性
    val ids = result.map(re => {
      re.getAs[mutable.WrappedArray[Double]]("id")
    }).filter(_ != null).flatMap(_.map(a => {
      a
    })).zipWithIndex().toDF("id", "key")

    val names = result.map(re => {
      re.getAs[mutable.WrappedArray[String]]("name")
    }).filter(_ != null).flatMap(_.map(a => {
      a
    })).zipWithIndex().toDF("name", "key")

    //30226数据有问题 30225:113268206
    val labels = result.map(re => {
      re.getAs[mutable.WrappedArray[mutable.WrappedArray[String]]]("labels")
    }).filter(_ != null).flatMap(_.map(a => {
      a.apply(0)
    })).zipWithIndex()

    val comlabels = labels.filter(_._1.equals("Company")).toDF("label", "key")

    val companys = comlabels.join(names, "key").join(ids, "key")

    //对数据进行去重操作
    val company = companys.select("id", "name", "label").distinct()

    company.cache()
    company.count()

    import org.apache.spark.sql.SaveMode
    company.write.format("parquet").mode(SaveMode.Append).save("hdfs://master:9000/hive/company_parquet")
    //val company_parquet=sqlContext.read.format("parquet").load("hdfs://master:9000/hive/company_parquet")

    import org.elasticsearch.spark._
    import org.elasticsearch.spark.sql._
    //保存到ES中，注意spark-defaul中需要配置ES的maste节点
    /**
      * spark.es.nodes                   10.18.67.111
      *spark.es.port                            9200
      */
    company.write.format("es").mode(SaveMode.Append).save("company_item/item")

    //company.save("company_item/item","es",SaveMode.Append)

    //company.registerTempTable("company")

  }
}

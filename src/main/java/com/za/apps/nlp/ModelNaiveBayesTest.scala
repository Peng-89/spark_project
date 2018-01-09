package com.za.apps.nlp

import com.hankcs.hanlp.dictionary.stopword.CoreStopWordDictionary
import com.hankcs.hanlp.tokenizer.StandardTokenizer
import com.za.apps.nlp.TFIDFKmeansInOneFile.transform
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.Row

import scala.collection.JavaConversions._

object ModelNaiveBayesTest {

  val modelpath = "D:\\workspace\\SparkProject\\data\\model\\naivebayes\\"

  case class RawDataRecord(category: String, words: List[String])

  /**
    * String 分词
    *
    * @param sentense
    * @return
    */
  def transform(sentense: String): List[String] = {
    val list = StandardTokenizer.segment(sentense)
    CoreStopWordDictionary.apply(list)
    list.map(x => x.word.replaceAll(" ", "")).toList
  }

  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "D:\\服务器管理\\平台软件\\hadoop-2.6.4\\hadoop-2.6.4")
    val conf = new SparkConf().setMaster("local[*]").setAppName("ModelNaiveBayesTest").set("executor-memory","4g")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val hashingTF = new HashingTF()
      .setNumFeatures(500000).setInputCol("words").setOutputCol("rawFeatures")
    var idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")

    var trainingDF = sc.textFile("D:\\workspace\\SparkProject\\data\\sougou-train\\*").map {
      x =>RawDataRecord(x.substring(0,1),transform(x.substring(2)))
    }.toDF()
    var featurizedData = hashingTF.transform(trainingDF)

    var idfModel = idf.fit(featurizedData)

    var testRdd = sc.textFile("D:\\workspace\\SparkProject\\data\\test_data.txt").map {
      x =>
        var data = x.split(",")
        RawDataRecord(data(0), transform(data(1)))
    }.toDF()

    val featurizedDatat = hashingTF.transform(testRdd)
    var rescaledData = idfModel.transform(featurizedDatat)
    var testDataRdd = rescaledData.select($"category", $"features").map {
      case Row(label: String, features: Vector) =>
        LabeledPoint(label.toDouble, Vectors.dense(features.toArray))
    }

    val nmodel = NaiveBayesModel.load(sc, modelpath)
    val testpredictionAndLabel2 = testDataRdd.map(p => (nmodel.predict(p.features), p.label))
    testpredictionAndLabel2.collect().foreach(println)
  }
}
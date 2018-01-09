package com.za.apps.ljemail

import com.hankcs.hanlp.dictionary.stopword.CoreStopWordDictionary
import com.hankcs.hanlp.tokenizer.StandardTokenizer
import com.za.apps.nlp.ModelNaiveBayes.modelpath
import com.za.apps.nlp.ModelNaiveBayesTest.{RawDataRecord, transform}
import org.apache.spark.ml.feature.{HashingTF, IDF}
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._

object ModelNaiveBayesEmail {
  val modelpath="D:\\workspace\\SparkProject\\data\\model\\naivebayesemail\\"

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

  def main(args : Array[String]) {

    System.setProperty("hadoop.home.dir", "D:\\服务器管理\\平台软件\\hadoop-2.6.4\\hadoop-2.6.4")

    val conf = new SparkConf().setMaster("local[*]").setAppName("ModelNaiveBayesEmail")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

//    var datadf = sc.textFile("D:\\workspace\\SparkProject\\data\\email_data\\*").map {
//      x =>RawDataRecord(x.substring(0,1),transform(x.substring(2)))
//    }.toDF()
//
//    datadf.repartition(20).write.mode(SaveMode.Overwrite).save("D:\\workspace\\SparkProject\\data\\email_data_rep")
    var datadf = sqlContext.read.load("D:\\workspace\\SparkProject\\data\\email_data_rep")

    val splits = datadf.randomSplit(Array(0.7, 0.3))
    var trainingDF = splits(0)
    var testDF = splits(1)

    var hashingTF = new HashingTF().setNumFeatures(500000).setInputCol("words").setOutputCol("rawFeatures")
    var featurizedData = hashingTF.transform(trainingDF)



    //计算每个词的TF-IDF
    var idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    var idfModel = idf.fit(featurizedData)
    var rescaledData = idfModel.transform(featurizedData)

    //转换成Bayes的输入格式
    var trainDataRdd = rescaledData.select($"category",$"features").map {
      case Row(label: String, features: Vector) =>
        LabeledPoint(label.toDouble, Vectors.dense(features.toArray))
    }

    //训练模型
    var model = NaiveBayes.train(trainDataRdd, lambda = 1.0, modelType = "multinomial")

//    //测试数据集，做同样的特征表示及格式转换
//    var testfeaturizedData = hashingTF.transform(testDF)
//    var testrescaledData = idfModel.transform(testfeaturizedData)
//    var testDataRdd = testrescaledData.select($"category",$"features").map {
//      case Row(label: String, features: Vector) =>
//        LabeledPoint(label.toDouble, Vectors.dense(features.toArray))
//    }
//
//    //对测试数据集使用训练模型进行分类预测
//    val testpredictionAndLabel = testDataRdd.map(p => (model.predict(p.features), p.label))
//
//
//    //    //统计分类准确率
//    var testaccuracy = 1.0 * testpredictionAndLabel.filter(x => x._1 == x._2).count() / testDataRdd.count()
//    println("output5：")
//    println(testaccuracy)

    model.save(sc,modelpath)
  }
}

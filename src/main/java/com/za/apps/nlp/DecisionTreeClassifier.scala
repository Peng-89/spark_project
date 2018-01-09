package com.za.apps.nlp

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer, HashingTF, Tokenizer}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.Row
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.log4j.{Level, Logger}

case class Iris(features: Vector, label: String)

object DecisionTreeClassifiers {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val sparkConf = new SparkConf().setAppName("Kmeans use tf-idf").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("ERROR")
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits
    val data2 = sc.textFile("D:\\workspace\\SparkProject\\data\\iris.data.txt").filter(_.split(",").size == 5)
      .map(_.split(",")).map(p => Iris(Vectors.dense(p(0).toDouble,p(1).toDouble,p(2).toDouble, p(3).toDouble), p(4).toString()))
    val data = sqlContext.createDataFrame(data2)
    data.show()
    val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(data)
    val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").fit(data)
    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))
    val dt = new DecisionTreeClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
      .setImpurity("gini")
      .setMaxDepth(5)
    println("DecisionTreeClassifier parameters:\n" + dt.explainParams() + "\n")

    val labelConverter = new IndexToString().
      setInputCol("prediction").
      setOutputCol("predictedLabel").
      setLabels(labelIndexer.labels)
    val pipeline = new Pipeline().
      setStages(Array(labelIndexer, featureIndexer, dt, labelConverter))
    val model = pipeline.fit(trainingData)
    val predictions = model.transform(testData)

    predictions.
      select("predictedLabel", "label", "features").
      collect().
      foreach { case Row(predictedLabel: String, label: String, features: Vector) =>
        println(s"($label, $features) --> predictedLabel=$predictedLabel")
      }

    val evaluator = new MulticlassClassificationEvaluator().
             setLabelCol("indexedLabel").
             setPredictionCol("prediction")
    val accuracy = evaluator.evaluate(predictions)
    println(s"Success ${accuracy}, Test Error = " + (1.0 - accuracy))

    val treeModel = model.stages(2).asInstanceOf[DecisionTreeClassificationModel]

    println("Learned classification tree model:\n" + treeModel.toDebugString)

    val test = sc.parallelize(List("6.1,2.9,4.7,1.4,Iris-versicolor")).map(_.split(",")).map(p => Iris(Vectors.dense(p(0).toDouble,
      p(1).toDouble,p(2).toDouble, p(3).toDouble),p(4).toString))
    val predictions2 = model.transform(sqlContext.createDataFrame(test))
    predictions2.
      select("predictedLabel", "label", "features").
      collect().
      foreach { case Row(predictedLabel: String, label: String, features: Vector) =>
        println(s"($label, $features) --> predictedLabel=$predictedLabel")
      }

  }
}

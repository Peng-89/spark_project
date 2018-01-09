package com.za.apps.recommend

import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.{SparkConf, SparkContext}

object MovieLensModel {
  val dataPath = "D:\\workspace\\SparkProject\\data\\ml-100k\\"

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\服务器管理\\平台软件\\hadoop-2.6.4\\hadoop-2.6.4")

    val conf = new SparkConf().setMaster("local[*]").setAppName("MovieLensModel").set("spark.executor.memory", "4g")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    //rating_data
    val rating_data = sc.textFile(dataPath + "u.data").map(_.split("\t").take(3))
    val ratings = rating_data.map {
      case Array(user, movie, rating) => Rating(user.toInt, movie.toInt, rating.toDouble)
    }
    val model = ALS.train(ratings,50,10,0.01)
//    println(model.userFeatures.count())
//    println(model.productFeatures.count())
    //用户推荐
    val predictedRating = model.predict(789,123)
    println(predictedRating)

    val topKRecs = model.recommendProducts(789,10)
    println(topKRecs.mkString("\n"))

    val titles = sc.textFile(dataPath + "u.item").map(line=>line.split("\\|").take(2)).map(array=>(array(0).toInt,array(1))).collectAsMap()

    val moviesForUser = ratings.keyBy(_.user).lookup(789)
    println(moviesForUser.size)
    moviesForUser.sortBy(-_.rating).take(10).map(rating=>(titles(rating.product),rating.rating)).foreach(println)
    //tuijian
    println("=======================tuijian")
    topKRecs.map(rating=>(titles(rating.product),rating.rating)).foreach(println)

    //物品推荐
    import org.jblas.DoubleMatrix
    val itemId=567
    val itemVector = new DoubleMatrix(model.productFeatures.lookup(itemId).head)
    def consineSimilarity(vec1:DoubleMatrix,vec2:DoubleMatrix) ={
      vec1.dot(vec2) /(vec1.norm2() * vec2.norm2())
    }
    println("=================物品推荐")
    println(consineSimilarity(itemVector,itemVector))

    val sims = model.productFeatures.map{
      case (id,factor)=>
        val factorVector = new DoubleMatrix(factor)
        val sim = consineSimilarity(factorVector,itemVector)
        (id,sim)
    }

    val sortedSims =sims.top(11)(Ordering.by[(Int,Double),Double]{
      case (id,similarity)=>similarity
    })

    println(sortedSims.mkString("\n"))

    println(titles(itemId))

    println("=================推荐")
    sortedSims.slice(1,11).map{case (id,sim)=>(titles(id),sim)}.foreach(println)


  }
}

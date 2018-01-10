package com.za.apps.recommend

import com.za.apps.recommend.MovieLensModel.dataPath
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.{SparkConf, SparkContext}

object MovieLensModelImplicit {
  val dataPath = "D:\\workspace\\SparkProject\\data\\ml-100k\\"

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\服务器管理\\平台软件\\hadoop-2.6.4\\hadoop-2.6.4")

    val conf = new SparkConf().setMaster("local[*]").setAppName("MovieLensModelImplicit").set("spark.executor.memory", "4g")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    //rating_data
    val rating_data = sc.textFile(dataPath + "u.data").map(_.split("\t").take(3))
    val ratings = rating_data.map {
      case Array(user, movie, rating) => {
        Rating(user.toInt, movie.toInt, if (rating.toDouble-2.5>0) rating.toDouble-2.5 else 0)
      }
    }
    val model = ALS.trainImplicit(ratings,50,10,0.01,0.01)

    val predictedRating = model.predict(789,123)
    println(predictedRating)
    val topKRecs = model.recommendProducts(789,10)
    println(topKRecs.mkString("\n"))

    val titles = sc.textFile(dataPath + "u.item").map(line=>line.split("\\|").take(2)).map(array=>(array(0).toInt,array(1))).collectAsMap()

    val moviesForUser = ratings.keyBy(_.user).lookup(789)
    println(moviesForUser.size)
    moviesForUser.sortBy(-_.rating).take(10).map(rating=>(titles(rating.product),rating.rating)).foreach(println)
    //tuijian
    println("=======================用户推荐商品")
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



    //评估模型的准确性
    /**
      * 均方差（MSE）
      */
    val actualRating = moviesForUser.take(1)(0)
    println(s"actualRating:${actualRating.toString}")
    val predicteRating = model.predict(789,actualRating.product)
    println(s"predicteRating:${predicteRating}")

    val squaredError = math.pow(predicteRating - actualRating.rating,2.0)
    println(s"squaredError:${squaredError}")

    //整体的均方差
    val userProducts = ratings.map{case Rating(user,product,rating)=>(user,product)}

    val predictions = model.predict(userProducts).map{case Rating(user,product,rating)=>((user,product),rating)}

    val ratingAndPredictions = ratings.map{
      case Rating(user,product,rating)=>((user,product),rating)
    }.join(predictions)

    val MSE = ratingAndPredictions.map{
      case ((user,product),(actual,predicted))=>math.pow(actual-predicted,2)
    }.reduce(_+_)/ratingAndPredictions.count()

    println(s"Mean Squared Error=${MSE}")

    //均方差根误差
    val RMSE = math.sqrt(MSE)
    println(s"Root Mean Squared Error=${RMSE}")

    /**
      * k值平均准确率
      */
    def avgPrecisionK(actual:Seq[Int],predicted:Seq[Int],k:Int):Double={
      val predK = predicted.take(k)
      var score=0.0
      var numHits =0.0
      for((p,i)<- predK.zipWithIndex){
        if(actual.contains(p)){
          numHits+=1.0
          score +=numHits/(i.toDouble+1.0)
        }
      }
      if(actual.isEmpty){
        1.0
      }else {
        score/ math.min(actual.size,k).toDouble
      }
    }

    val actualMovies = moviesForUser.map(_.product)
    val predictedMovies = topKRecs.map(_.product)
    val apk10 = avgPrecisionK(actualMovies,predictedMovies,10)
    println(s"apk10=${apk10}")


    val itemFactors = model.productFeatures.map{case (id,factor)=>factor}.collect()
    val itemMatrix = new DoubleMatrix(itemFactors)
    println(itemMatrix.rows,itemMatrix.columns)

    val imBroadcast = sc.broadcast(itemMatrix)

    val allRecs = model.userFeatures.map{case (userId,array)=>
      val userVector = new DoubleMatrix(array)
      val scores =imBroadcast.value.mmul(userVector)
      val recommendedIds = scores.data.zipWithIndex.sortBy(-_._1).map(_._2+1).toSeq
      (userId,recommendedIds)
    }

    val userMovies = ratings.map{case Rating(user,product,rating)=>(user,product)}.groupBy(_._1)

    val k =10
    val MAPK = allRecs.join(userMovies).map{case (userId,(predicted,actualWithIds))=>
      val actual = actualWithIds.map(_._2).toSeq
      avgPrecisionK(actual,predicted,k)
    }.reduce(_+_)/allRecs.count()
    println(s"Mean Average Precision at K =${MAPK}")

    //使用MLib内置的评估函数，需要记录每一条记录对应每个数据点上的相应的预测值和实际值
    val predictedAndTrue = ratingAndPredictions.map{case ((user,product),(actual,predicted))=>(predicted,actual)}
    val regressionMetrics = new RegressionMetrics(predictedAndTrue)
    println(s"Mean Squared Error = ${regressionMetrics.meanSquaredError}")
    println(s"Root Mean Squared Error = ${regressionMetrics.rootMeanSquaredError}")
  }
}

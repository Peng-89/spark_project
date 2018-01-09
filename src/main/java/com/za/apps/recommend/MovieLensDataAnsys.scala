package com.za.apps.recommend

import org.apache.spark.{SparkConf, SparkContext}

object MovieLensDataAnsys {
  val dataPath = "D:\\workspace\\SparkProject\\data\\ml-100k\\"
  def main(args : Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\服务器管理\\平台软件\\hadoop-2.6.4\\hadoop-2.6.4")

    val conf = new SparkConf().setMaster("local[*]").setAppName("MovieLensDataAnsys").set("spark.executor.memory","4g")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

  /*  //user_data
    val user_data = sc.textFile(dataPath+"u.user")
    user_data.take(1).foreach(print)

    println()

    val user_fields = user_data.map(_.split("\\|"))
    println(user_fields.map(a=>a(0)).count())
    println(user_fields.map(a=>a(1)).distinct().count())
    println(user_fields.map(a=>a(2)).distinct().count())
    println(user_fields.map(a=>a(3)).distinct().count())
    println(user_fields.map(a=>a(4)).distinct().count())

    //age fenbu
    //user_fields.groupBy(a=>a(1)).map(a=>(a._1,a._2.size)).sortByKey().foreach(println)
    //user_fields.groupBy(a=>a(2)).map(a=>(a._1,a._2.size)).sortByKey().foreach(println)
    //user_fields.groupBy(a=>a(3)).map(a=>(a._1,a._2.size)).sortBy(_._2.toDouble).foreach(println)
    user_fields.map(a=>a(3)).countByValue().toList.sortBy(_._2).foreach(println) */

    //movie_data
   /* val movie_data = sc.textFile(dataPath+"u.item")
    movie_data.take(1).foreach(print)
    println()
    println(movie_data.count())

    def convert_year(x:String):Int={
       if(null != x && x.split("-").length==3){
         x.split("-")(2).toInt
       }else {
         1990
       }
    }
    val movie_fields = movie_data.map(_.split("\\|"))
    movie_fields.map(a=>convert_year(a(2))).filter(_!=1990).map(1998-_).countByValue().toList.sortBy(_._2).foreach(println)
    */
    //rating_data
   val rating_data = sc.textFile(dataPath+"u.data")
    rating_data.take(1).foreach(print)
    println()
    println(rating_data.count())
    val movie_fields = rating_data.map(_.split("\t"))
    val ratings = movie_fields.map(_(2).toInt)
    println(ratings.stats().toString())
    ratings.countByValue().toList.sortBy(_._2).foreach(println)

    println("=========users_ratings")
    val users_ratings = movie_fields.map(a=>(a(0).toInt,a(2).toInt))
    users_ratings.groupByKey().map(a=>(a._1,a._2.size)).take(5).foreach(println)
  }
}

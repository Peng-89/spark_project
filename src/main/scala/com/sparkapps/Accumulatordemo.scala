package com.sparkapps

import org.apache.spark.{AccumulatorParam, SparkConf, SparkContext}

/**
  * 累加器使用
  */
object Accumulatordemo {
  def main(args: Array[String]): Unit = {
     val conf = new SparkConf().setAppName("Accumulatordemo")
                                .setMaster("local")
     val sc = new SparkContext(conf)
     val data = sc.parallelize((1 to 10),3)
     val accum = sc.accumulator(0,"accumulator")

     //自定义计数器，用来保存相关对象
    val accumMy = sc.accumulator(Array[Int]())(MyAccumulator)

     val cc=data.map(d=>{
       accum +=d
       accumMy.add(Array(d))
     })
    //使用cache缓存数据，切断依赖。
    cc.cache()
    println(cc.count)
    println(accum.value)
    accumMy.value.foreach(print)
    println()

    /**
      *  使用累加器的过程中只能使用一次action的操作才能保证结果的准确性,如果需要多次使用，需要将transform的依赖链断掉，使用cache，persist
      * 只要将任务之间的依赖关系切断就可以了。什么方法有这种功能呢？你们肯定都想到了，cache，persist。调用这个方法的时候会将之前的依赖切除，后续的累加器就不会再被之前的transfrom操作影响到了
      */
    println(cc.count)
    println(accum.value)
    sc.stop()
  }
}

object MyAccumulator extends AccumulatorParam[Array[Int]]{
  override def addInPlace(r1: Array[Int], r2: Array[Int]): Array[Int] = {
       //println(r1.toSeq+","+r2.toSeq)
       r1.union(r2)
  }

  override def zero(initialValue: Array[Int]): Array[Int] = {
    //println("init:"+initialValue.toSeq)
    initialValue
  }
}

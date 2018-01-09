package com.sparkapps

/**
  * 二次排序的key
  */
class SortSecondKey(val first:Int,val second:Int) extends Ordered[SortSecondKey] with Serializable{
  override def compare(that: SortSecondKey): Int = {
    if(this.first-that.first!=0){
      this.first-that.first
    }else{
      this.second-that.second
    }
  }
}

object SortSecondKey{
  def apply(first: Int, second: Int): SortSecondKey = new SortSecondKey(first, second)
}
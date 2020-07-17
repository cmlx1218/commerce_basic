package com.atguigu.session

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
  * @Desc
  * @Author cmlx
  * @Date 2020-7-17 0017 14:41
  */
class SessionAccumulator extends AccumulatorV2[String, mutable.HashMap[String, Int]] {
  override def isZero: Boolean = ???

  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Int]] = ???

  override def reset(): Unit = ???

  override def add(v: String): Unit = ???

  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Int]]): Unit = ???

  override def value: mutable.HashMap[String, Int] = ???
}

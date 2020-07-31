package com.atguigu.session.friend

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Desc spark共同好友查询
  * @Author cmlx
  * @Date 2020-7-30 0030 15:38
  */
object FindCommenFriends {

  def main(args: Array[String]): Unit = {

    //测试环境使用一个内核即可，生产环境中进行修改
    val sparkConf: SparkConf = new SparkConf()
      .setMaster("local[1]")
      .setAppName("FindCommonFriends")
    val sc: SparkContext = SparkContext.getOrCreate(sparkConf)

    //1、获取原始的好友数据
    val records: RDD[String] = sc.textFile("session/src/main/resources/friend/friends.txt")
    records.foreach(println(_))

    //2、映射两两组合键值对
    val pairs: RDD[((String, String), Seq[String])] = records.flatMap(record => {
      val tokens: Array[String] = record.split(",")
      val person: String = tokens(0)
      val friends: Seq[String] = tokens(1).split("\\s").toList
      val result: Seq[((String, String), Seq[String])] = for {
        i <- 0 until friends.size
        friend = friends(i)
      } yield {
        if (person < friend)
          ((person, friend), friends)
        else
          ((friend, person), friends)
      }
      result
    });
    pairs.foreach(println(_))

    pairs.groupByKey().foreach(println(_))
    println("-----------------------------------------")

    //3、共同好友计算
    val commonFriends: RDD[((String, String), Iterable[String])] = pairs
      .groupByKey()
      .mapValues(item => {
        val friendCount = for {
          list <- item
          if list.nonEmpty
          friend <- list
        } yield ((friend, 1))


        //获取共同好友
        //        friendCount.groupBy(_._1).mapValues(_.unzip._2.sum).filter(_._2 > 1).map(_._1)
        //获取推荐好友【包含参考双方】
        friendCount.groupBy(_._1).mapValues(_.unzip._2.sum).filter(_._2 <= 1).map(_._1)
      })

    println("获取推荐好友【包含参考双方】：=======================》")
    commonFriends.foreach(println(_))

    val recommendFriends: RDD[((String, String), Iterable[String])] = commonFriends.map {
      case ((person, friend), commendFriend) => {
        val disCommendFriend = commendFriend.filterNot(c => c.contains(person) || c.contains(friend))

        ((person, friend), disCommendFriend)

      }
    }

    println("获取推荐好友【不包含参考双方】：=======================》")
    recommendFriends.foreach(println(_))


    //打印共同好友结果
    val formatedResult = commonFriends.map(
      f => s"(${f._1._1}, ${f._1._2})\t${f._2.mkString("[", ", ", "]")}"
    )

    //    formatedResult.foreach(println(_))
    sc.stop()

  }
}

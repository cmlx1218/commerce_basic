package com.atguigu.session

import java.util.UUID

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.UserVisitAction
import commons.utils.ParamUtils
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @Desc
  * @Author cmlx
  * @Date 2020-7-14 0014 15:34
  */
object SessionStat {

  def main(args: Array[String]): Unit = {
    //获取筛选条件
    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)

    //获取筛选条件对应的JsonObject
    val taskParam = JSONObject.fromObject(jsonStr)

    //创建全局唯一的主键
    val taskUUID = UUID.randomUUID().toString

    //创建SparkConf
    val sparkConf = new SparkConf().setAppName("session").setMaster("local[*]")

    //创建SparkSession
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    //获取原始的动作表数据
    //actionRDD:RDD[UserVisitAction]
    val actionRDD = getOriActionRDD(sparkSession, taskParam)
//    actionRDD.foreach(println(_))

    //sessionId2ActionRDD[(sessionId,UserVisitAction)]
    val sessionId2ActionRDD = actionRDD.map(item => (item.session_id, item))

    //session2GroupActionRDD:RDD[(sessionId,iterable_UserVisitAction)]
    val session2GroupActionRDD = sessionId2ActionRDD.groupByKey()

    session2GroupActionRDD.cache()

    session2GroupActionRDD.foreach(println(_))

  }

  def getOriActionRDD(sparkSession: SparkSession, taskParam: JSONObject) = {
    val startData = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endData = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)
    val sql = "select * from user_visit_action where date >= '" + startData + "' and date<='" + endData + "'"
    import sparkSession.implicits._
    sparkSession.sql(sql).as[UserVisitAction].rdd
  }

}

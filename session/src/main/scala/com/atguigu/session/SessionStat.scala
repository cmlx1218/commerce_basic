package com.atguigu.session

import java.util.{Date, UUID}

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.{UserInfo, UserVisitAction}
import commons.utils.{DateUtils, NumberUtils, ParamUtils, StringUtils, ValidUtils}
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

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
    //actionRDD.foreach(println(_))

    //将sessionId对应UserVisitAction
    //sessionId2ActionRDD[(sessionId,UserVisitAction)]
    val sessionId2ActionRDD = actionRDD.map(item => (item.session_id, item))

    //将sessionId相同的整合在一起
    //session2GroupActionRDD:RDD[(sessionId,iterable_UserVisitAction)]
    val session2GroupActionRDD = sessionId2ActionRDD.groupByKey()

    session2GroupActionRDD.cache()

    //    session2GroupActionRDD.foreach(println(_))
    // 将sessionId相同的数据处理聚合到一起(33,sessionid=842c902b86cf40a2ac795d6294c337ad|searchKeywords=|clickCategoryIds=9|visitLength=3153|stepLength=4|startTime=2020-07-14 17:04:28)
    val sessionId2FullInfoRDD = getSessionFullInfo(sparkSession, session2GroupActionRDD)

    //    sessionId2FullInfoRDD.foreach(println(_))


    // 注册自定义累加器
    val sessionAccumulator = new SessionAccumulator
    sparkSession.sparkContext.register(sessionAccumulator)
    // 过滤数据【实现根据限制条件对session数据进行过滤，并完成累加器的更新】
    val sessionId2FilterRDD = getSessionFilteredRDD(taskParam, sessionId2FullInfoRDD, sessionAccumulator)
    sessionId2FilterRDD.foreach(println(_))
    // 从累加器中读取各个访问步长和访问时长的session的个数，计算比率，并将最终的结果写入数据库
    getSessionRatio(sparkSession, taskUUID, sessionAccumulator.value)

    /**
      * 需求二：session随机抽取
      * sessionId2FilterRDD: RDD[(sid, fullInfo)]
      * 一个session对应一条数据，也就是一个fullInfo
      */
    sessionRandomExtract(sparkSession, taskUUID, sessionId2FilterRDD)

  }

  def generateRandomIndexList(extractPerDay: Long,
                              daySessionCount: Long,
                              hourCountMap: mutable.HashMap[String, Long],
                              hourListMap: mutable.HashMap[String, ListBuffer[Int]]): Unit = {
    for ((hour, count) <- hourCountMap) {
      // 获取一个小时要抽取多少条数据
      var hourExrCount = ((count / daySessionCount.toDouble) * extractPerDay).toInt
      // 避免一个小时要抽取的数量超过这个歌小时的总数
      if (hourExrCount > count)
        hourExrCount = count.toInt

      val random = new Random()

      hourListMap.get(hour) match {
        case None => hourListMap(hour) = new ListBuffer[Int]
          for (i <- 0 until hourExrCount) {
            var index = random.nextInt(count.toInt)
            while (hourListMap(hour).contains(index)) {
              index = random.nextInt(count.toInt)
            }
            hourListMap(hour).append(index)
          }
        case Some(list) =>
          for (i <- 0 until hourExrCount) {
            var index = random.nextInt(count.toInt)
            while (hourListMap(hour).contains(index)) {
              index = random.nextInt(count.toInt)
            }
            hourListMap(hour).append(index)
          }
      }
    }
  }


  def sessionRandomExtract(sparkSession: SparkSession, taskUUID: String, sessionId2FilterRDD: RDD[(String, String)]): Unit = {

    // dateHour2FullInfoRDD: RDD[(dateHour, fullInfo)]
    val dateHour2FullInfoRDD = sessionId2FilterRDD.map {
      case (sid, fullInfo) =>
        val startTime = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_START_TIME)
        // dateHour: yyyy-MM-dd_HH
        val dateHour = DateUtils.getDateHour(startTime)
        (dateHour, fullInfo)
    }
    // hourCountMap: Map[(dateHour, count)]
    val hourCountMap = dateHour2FullInfoRDD.countByKey()
    // dateHourCountMap: Map[date,Map[hour,count]]
    val dateHourCountMap = new mutable.HashMap[String, mutable.HashMap[String, Long]]()

    for ((dateHour, count) <- hourCountMap) {
      val date = dateHour.split("_")(0)
      val hour = dateHour.split("_")(1)

      dateHourCountMap.get(date) match {
        case None => dateHourCountMap(date) = new mutable.HashMap[String, Long]()
          dateHourCountMap(date) += (hour -> count)
        case Some(map) => dateHourCountMap(date) += (hour -> count)
      }
    }

    // 解决问题一：一共有多少天：dateHourCountMap.size
    //          一天抽取多少条：100 / dateHourCountMap.size
    val extractPerDay = 100 / dateHourCountMap.size
    //解决问题二：一天有多少个session：dateHourCountMap(date).values.sum
    //解决问题三：一个小时有多少session：dateHourCountMap(date)(hour)
    val dateHourExtractIndexListMap = new mutable.HashMap[String, mutable.HashMap[String, ListBuffer[Int]]]()

    for ((date, hourCountMap) <- dateHourCountMap) {
      val dateSessionCount = hourCountMap.values.sum

      dateHourExtractIndexListMap.get(date) match {
        case None => dateHourExtractIndexListMap(date) = new mutable.HashMap[String, ListBuffer[Int]]()
          generateRandomIndexList(extractPerDay, dateSessionCount, hourCountMap, dateHourExtractIndexListMap(date))
        case Some(map) => {
          generateRandomIndexList(extractPerDay, dateSessionCount, hourCountMap, dateHourExtractIndexListMap(date))
        }

          // 到目前为止，我们获得了每个小时要抽取的session的index
          // 广播大变量，提升人物性能
          val dateHourExtractIndexListMapBd = sparkSession.sparkContext.broadcast(dateHourExtractIndexListMap)

          //

      }
    }

  }


  def getSessionRatio(sparkSession: SparkSession, taskUUID: String, value: mutable.HashMap[String, Int]): Unit = {

    val session_count = value.getOrElse(Constants.SESSION_COUNT, 1).toDouble
    val visitLength_1s_3s = value.getOrElse(Constants.TIME_PERIOD_1s_3s, 0)
    val visitLength_4s_6s = value.getOrElse(Constants.TIME_PERIOD_4s_6s, 0)
    val visitLength_7s_9s = value.getOrElse(Constants.TIME_PERIOD_7s_9s, 0)
    val visitLength_10s_30s = value.getOrElse(Constants.TIME_PERIOD_10s_30s, 0)
    val visitLength_30s_60s = value.getOrElse(Constants.TIME_PERIOD_30s_60s, 0)
    val visitLength_1m_3m = value.getOrElse(Constants.TIME_PERIOD_1m_3m, 0)
    val visitLength_3m_10m = value.getOrElse(Constants.TIME_PERIOD_3m_10m, 0)
    val visitLength_10m_30m = value.getOrElse(Constants.TIME_PERIOD_10m_30m, 0)
    val visitLength_30m = value.getOrElse(Constants.TIME_PERIOD_30m, 0)

    val stepLength_1_3 = value.getOrElse(Constants.STEP_PERIOD_1_3, 0)
    val stepLength_4_6 = value.getOrElse(Constants.STEP_PERIOD_4_6, 0)
    val stepLength_7_9 = value.getOrElse(Constants.STEP_PERIOD_7_9, 0)
    val stepLength_10_30 = value.getOrElse(Constants.STEP_PERIOD_10_30, 0)
    val stepLength_30_60 = value.getOrElse(Constants.STEP_PERIOD_30_60, 0)
    val stepLength_60 = value.getOrElse(Constants.STEP_PERIOD_60, 0)

    val visit_length_1s_3s_ratio = NumberUtils.formatDouble(visitLength_1s_3s / session_count, 2)
    val visit_length_4s_6s_ratio = NumberUtils.formatDouble(visitLength_4s_6s / session_count, 2)
    val visit_length_7s_9s_ratio = NumberUtils.formatDouble(visitLength_7s_9s / session_count, 2)
    val visit_length_10s_30s_ratio = NumberUtils.formatDouble(visitLength_10s_30s / session_count, 2)
    val visit_length_30s_60s_ratio = NumberUtils.formatDouble(visitLength_30s_60s / session_count, 2)
    val visit_length_1m_3m_ratio = NumberUtils.formatDouble(visitLength_1m_3m / session_count, 2)
    val visit_length_3m_10m_ratio = NumberUtils.formatDouble(visitLength_3m_10m / session_count, 2)
    val visit_length_10m_30m_ratio = NumberUtils.formatDouble(visitLength_10m_30m / session_count, 2)
    val visit_length_30m_ratio = NumberUtils.formatDouble(visitLength_30m / session_count, 2)

    val step_length_1_3_ratio = NumberUtils.formatDouble(stepLength_1_3 / session_count, 2)
    val step_length_4_6_ratio = NumberUtils.formatDouble(stepLength_4_6 / session_count, 2)
    val step_length_7_9_ratio = NumberUtils.formatDouble(stepLength_7_9 / session_count, 2)
    val step_length_10_30_ratio = NumberUtils.formatDouble(stepLength_10_30 / session_count, 2)
    val step_length_30_60_ratio = NumberUtils.formatDouble(stepLength_30_60 / session_count, 2)
    val step_length_60_ratio = NumberUtils.formatDouble(stepLength_60 / session_count, 2)

    // 将统计结果封装为Domain对象
    val sessionAggrStat = SessionAggrStat(taskUUID,
      session_count.toInt, visit_length_1s_3s_ratio, visit_length_4s_6s_ratio, visit_length_7s_9s_ratio,
      visit_length_10s_30s_ratio, visit_length_30s_60s_ratio, visit_length_1m_3m_ratio,
      visit_length_3m_10m_ratio, visit_length_10m_30m_ratio, visit_length_30m_ratio,
      step_length_1_3_ratio, step_length_4_6_ratio, step_length_7_9_ratio,
      step_length_10_30_ratio, step_length_30_60_ratio, step_length_60_ratio)

    //将数据写入mysql数据库
    val sessionRatioRDD = sparkSession.sparkContext.makeRDD(Array(sessionAggrStat))

    import sparkSession.implicits._

    sessionRatioRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "session_stat_ratio_1218")
      .mode(SaveMode.Append)
      .save()

  }


  def calculateVisitLength(visitLength: Long, sessionAccumulator: SessionAccumulator) = {
    if (visitLength >= 1 && visitLength <= 3) {
      sessionAccumulator.add(Constants.TIME_PERIOD_1m_3m)
    } else if (visitLength >= 4 && visitLength <= 6) {
      sessionAccumulator.add(Constants.TIME_PERIOD_4s_6s)
    } else if (visitLength >= 7 && visitLength <= 9) {
      sessionAccumulator.add(Constants.TIME_PERIOD_7s_9s)
    } else if (visitLength >= 10 && visitLength <= 30) {
      sessionAccumulator.add(Constants.TIME_PERIOD_10s_30s)
    } else if (visitLength > 30 && visitLength <= 60) {
      sessionAccumulator.add(Constants.TIME_PERIOD_30s_60s)
    } else if (visitLength > 60 && visitLength <= 180) {
      sessionAccumulator.add(Constants.TIME_PERIOD_1m_3m)
    } else if (visitLength > 180 && visitLength <= 600) {
      sessionAccumulator.add(Constants.TIME_PERIOD_3m_10m)
    } else if (visitLength > 600 && visitLength <= 1800) {
      sessionAccumulator.add(Constants.TIME_PERIOD_10m_30m)
    } else if (visitLength > 1800) {
      sessionAccumulator.add(Constants.TIME_PERIOD_30m)
    }
  }

  def calculateStepLength(stepLength: Long, sessionAccumulator: SessionAccumulator) = {
    if (stepLength >= 1 && stepLength <= 3) {
      sessionAccumulator.add(Constants.STEP_PERIOD_1_3)
    } else if (stepLength >= 4 && stepLength <= 6) {
      sessionAccumulator.add(Constants.STEP_PERIOD_4_6)
    } else if (stepLength >= 7 && stepLength <= 9) {
      sessionAccumulator.add(Constants.STEP_PERIOD_7_9)
    } else if (stepLength >= 10 && stepLength <= 30) {
      sessionAccumulator.add(Constants.STEP_PERIOD_10_30)
    } else if (stepLength > 30 && stepLength <= 60) {
      sessionAccumulator.add(Constants.STEP_PERIOD_30_60)
    } else if (stepLength > 60) {
      sessionAccumulator.add(Constants.STEP_PERIOD_60)
    }
  }

  def getSessionFilteredRDD(taskParam: JSONObject,
                            sessionId2FullInfoRDD: RDD[(String, String)],
                            sessionAccumulator: SessionAccumulator) = {
    val startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE)
    val endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE)
    val professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS)
    val cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES)
    val sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX)
    val keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS)
    val categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS)

    var filterInfo = (if (startAge != null) Constants.PARAM_START_AGE + "=" + startAge + "|" else "") +
      (if (endAge != null) Constants.PARAM_END_AGE + "=" + endAge + "|" else "") +
      (if (professionals != null) Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" else "") +
      (if (cities != null) Constants.PARAM_CITIES + "=" + cities + "|" else "") +
      (if (sex != null) Constants.PARAM_SEX + "=" + sex + "|" else "") +
      (if (keywords != null) Constants.PARAM_KEYWORDS + "=" + keywords + "|" else "") +
      (if (categoryIds != null) Constants.PARAM_CATEGORY_IDS + "=" + categoryIds + "|" else "")

    if (filterInfo.endsWith("\\|"))
      filterInfo = filterInfo.substring(0, filterInfo.length - 1)

    sessionId2FullInfoRDD.filter {
      case (sessionId, fullInfo) => {
        var success = true
        if (!ValidUtils.between(fullInfo, Constants.FIELD_AGE, filterInfo, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
          success = false
        } else if (!ValidUtils.in(fullInfo, Constants.FIELD_PROFESSIONAL, filterInfo, Constants.PARAM_PROFESSIONALS)) {
          success = false
        } else if (!ValidUtils.in(fullInfo, Constants.FIELD_CITY, filterInfo, Constants.PARAM_CITIES)) {
          success = false
        } else if (!ValidUtils.equal(fullInfo, Constants.FIELD_SEX, filterInfo, Constants.PARAM_SEX)) {
          success = false
        } else if (!ValidUtils.in(fullInfo, Constants.FIELD_SEARCH_KEYWORDS, filterInfo, Constants.PARAM_KEYWORDS)) {
          success = false
        } else if (!ValidUtils.in(fullInfo, Constants.FIELD_CLICK_CATEGORY_IDS, filterInfo, Constants.PARAM_CATEGORY_IDS)) {
          success = false
        }

        if (success) {
          //acc.add()
          // session数量加1
          sessionAccumulator.add(Constants.SESSION_COUNT)
          // session属于哪个时长范围，给对应范围加1
          val visitLength = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_VISIT_LENGTH).toLong
          // session属于哪个步长范围，给对应范围加1
          val stepLength = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_STEP_LENGTH).toLong
          calculateVisitLength(visitLength, sessionAccumulator)
          calculateStepLength(stepLength, sessionAccumulator)
        }
        success
      }
    }

  }


  def getSessionFullInfo(sparkSession: SparkSession,
                         session2GroupActionRDD: RDD[(String, Iterable[UserVisitAction])]) = {

    //userId2AggrInfoRDD:RDD[(userId.aggrInfo)]
    val userId2AggrInfoRDD = session2GroupActionRDD.map {
      case (sessionId, iterableAction) =>
        var userId = -1L
        var startTime: Date = null
        var endTime: Date = null
        var stepLength = 0
        val searchKeywords = new StringBuffer("")
        val clickCategories = new StringBuffer("")

        for (action <- iterableAction) {
          if (userId == -1L) {
            userId = action.user_id
          }

          val actionTime = DateUtils.parseTime(action.action_time)
          if (startTime == null || startTime.after(actionTime)) {
            startTime = actionTime
          }
          if (endTime == null || endTime.before(actionTime)) {
            endTime = actionTime
          }

          val searchKeyword = action.search_keyword
          if (StringUtils.isNotEmpty(searchKeyword) && !searchKeywords.toString.contains(searchKeyword)) {
            searchKeywords.append(searchKeyword + ",")
          }

          val clickCategoryId = action.click_category_id
          if (clickCategoryId != -1 && !clickCategoryId.toString.contains(clickCategoryId)) {
            clickCategories.append(clickCategoryId + ",")
          }

          stepLength += 1
        }

        val searchKw = StringUtils.trimComma(searchKeywords.toString)
        val clickCg = StringUtils.trimComma(clickCategories.toString)
        val visitLength = (endTime.getTime - startTime.getTime) / 1000

        //聚合信息数据
        val aggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionId + "|" +
          Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKw + "|" +
          Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCg + "|" +
          Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" +
          Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|" +
          Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime)

        (userId, aggrInfo)
    }

    val sql = "select * from user_info"

    import sparkSession.implicits._
    val userId2InfoRDD = sparkSession.sql(sql).as[UserInfo].rdd.map(item => (item.user_id, item))

    val sessionId2FullInfoRDD = userId2AggrInfoRDD.join(userId2InfoRDD).map {
      case (userId, (aggrInfo, userInfo)) => {
        val age = userInfo.age
        val professional = userInfo.professional
        val sex = userInfo.sex
        val city = userInfo.city

        val fullInfo = aggrInfo + "|" +
          Constants.FIELD_AGE + "=" + age + "|" +
          Constants.FIELD_PROFESSIONAL + "=" + professional + "|" +
          Constants.FIELD_SEX + "=" + sex + "|" +
          Constants.FIELD_CITY + "=" + city

        val sessionId = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_SESSION_ID)
        (sessionId, fullInfo)
      }
    }
    sessionId2FullInfoRDD
  }


  def getOriActionRDD(sparkSession: SparkSession, taskParam: JSONObject) = {
    val startData = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endData = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)
    val sql = "select * from user_visit_action where date >= '" + startData + "' and date<='" + endData + "'"
    import sparkSession.implicits._
    sparkSession.sql(sql).as[UserVisitAction].rdd
  }

}


































package com.siva.sparkchallenge

import java.text.SimpleDateFormat

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.apache.spark.storage.StorageLevel

case class WebSchema(timestamp: String, elb: String, visitorIp: String, backendIp: String, requestURLProcessingTime: String, backendProcessingTime: String
                     , responseProcessingTime: String, elbStatusCode: String, backendStatusCode: String, receivedBytes: String
                     , sentBytes: String, requestURL: String, userAgent: String, sslCipher: String
                     , sslProtocol: String)

object WebLogAnalysis {

  def readCSV(spark: SparkSession, schema: StructType, filePath: String): DataFrame = {
    val csvReadConfig = Map("header" -> "false", "inferSchema" -> "false", "delimiter" -> " ", "header" -> "false", "quote" -> "\"", "escape" -> "\"", "encoding" -> "UTF-8")

    spark.read.options(csvReadConfig).schema(schema).csv(filePath)
  }

  def dataValidation() (df: DataFrame): DataFrame = {

    val timestampPattern = """^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}Z$"""
    val df1 = df.withColumn("timestamp", regexp_extract(col("timestamp"), timestampPattern, 0))

    val pat ="""([01]?[0-9]?[0-9]|2[0-4][0-9]|25[0-5])\"""
    val ipv4Pattern ="^"+pat+"."+pat+"."+pat+"."+pat+":"+"([0-9]{1,5})$".stripMargin
    val df2 = df1.withColumn("visitorIp", regexp_extract(col("visitorIp"), ipv4Pattern, 0))

    df2
  }

  def convertEpochDate(date: String) = {
    new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").parse(date).getTime
  }

  val epochUdf = udf(convertEpochDate(_: String): Long)
  //  val epochUdf = udf((date:String) => new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").parse(date).getTime)

  def cleanEpoch(df: DataFrame, col1: String, col2: String): DataFrame = {
    df.withColumn(col1, coalesce(col(col1), col(col2)))
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]").appName("WebLogAnalysis").getOrCreate()
    spark.udf.register("epochUdf", epochUdf)
    import spark.implicits._
    val webSchema = Encoders.product[WebSchema].schema

    //    read CSV file
    val logDf = readCSV(spark, webSchema, filePath = "data/2015_07_22_mktplace_shop_web_log_sample.log")

    val dataValidationDf = logDf.transform(dataValidation())

    //   convert timestamp into epoch and add new column epoch
    val selectLogDf = dataValidationDf.select("timestamp", "visitorIp", "requestURL", "userAgent")
      .withColumn("epochSec", epochUdf(col("timestamp")))
    //.withColumn("epoch_sec", unix_timestamp(to_timestamp(col("timestamp"),"yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'")))

    val windowFn = Window.partitionBy("visitorIp").orderBy("epochSec")
    //    find previous login time using lag
    val logWithEpochDf = selectLogDf.withColumn("prevEpoch", lag($"epochSec", 1).over(windowFn))
    //    remove null value from prev_epoch column
    val cleanEpochLogDf = logWithEpochDf.transform(cleanEpoch(_, "prevEpoch", "epochSec"))
    //    Login duration in millisecond
    val loginDuration = cleanEpochLogDf.withColumn("loginDuration", col("epochSec") - col("prevEpoch"))
    //    Calculate duration and identify the new session, assume every new session began at 15 minutes once
    //    15 min => 15*60*1000 (900000) milli seconds
    val identifyNewSession = loginDuration.withColumn("isNewSession", when($"loginDuration" > 900000, 1).otherwise(0))
    //    Identify the session Index
    val sessionIndexDf = identifyNewSession.withColumn("sessionIdx", sum("isNewSession").over(windowFn).cast("string"))
    val sessionIdLogDf = sessionIndexDf.withColumn("sessionId", concat_ws("_", $"visitorIp", $"sessionIdx"))

    //    Sessionize the web log by IP. Sessionize = aggregrate all page hits by visitor/IP during a session
    val sessionIdLogFinalDf = sessionIdLogDf.select("visitorIp", "sessionId", "loginDuration", "requestURL", "userAgent")
      .persist(StorageLevel.MEMORY_AND_DISK)
    //    sessionIdLogDf.filter($"sessionIdx" > 0).show(false)

    //    Determine the average session time
    val averageSessionDf = sessionIdLogFinalDf.agg(mean($"loginDuration").divide(60*1000).as("averageSessionTime"))

    //    Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.
    val urlVisitPerPersonDf = sessionIdLogFinalDf.select($"sessionId", split($"requestURL", " ").getItem(1) as ("splitURLLink"))
      .groupBy("sessionId").agg(collect_set("splitURLLink").as("requestURLArray"))
    val uniqueURLHitCnt = urlVisitPerPersonDf.select($"sessionId", size($"requestURLArray") as "hitCount")

    //    Find the most engaged users, ie the IPs with the longest session times
    val windowFn2 = Window.orderBy($"totalDurationPerUser" desc)
    val totalDurationPerUser = sessionIdLogFinalDf.groupBy($"visitorIp").sum("loginDuration")
    val mostEngagedUser = totalDurationPerUser.select(col("visitorIp"), col("sum(loginDuration)") as "totalDurationPerUser")
      .withColumn("durationRank", dense_rank().over(windowFn2)).filter($"durationRank" === 1)

    sessionIdLogFinalDf.show(false)
    averageSessionDf.show(false)
    uniqueURLHitCnt.show(false)
    mostEngagedUser.show()

  }
}

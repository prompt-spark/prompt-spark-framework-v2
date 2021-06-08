package com.example.dataengineering.data.layer.jobs
import org.apache.log4j.lf5.LogLevel
import org.apache.spark.sql.SparkSession
trait SparkSessionForDataLayerJobs {
  def newSparkSession: SparkSession = {
    val spark: SparkSession = SparkSession
      .builder()
      .enableHiveSupport()
      .appName(getClass.getName)
      .getOrCreate()
    spark.sparkContext.setLogLevel(LogLevel.WARN.toString)
    spark
  }
}

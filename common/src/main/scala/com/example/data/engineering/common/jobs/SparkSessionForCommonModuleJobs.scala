package com.example.data.engineering.common.jobs

import org.apache.log4j.lf5.LogLevel
import org.apache.spark.sql.SparkSession

trait SparkSessionForCommonModuleJobs {
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

package com.example.dataengineering.data.layer

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, Suite}

trait SparkSpec extends BeforeAndAfterAll {
  this: Suite =>

  private var _sc: SparkContext = _
  private var _ss: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    val conf = new SparkConf()
    //.setMaster("spark://44.196.21.43:7077")
      .setMaster("local[*]")
      //.set("spark.ui.port","7077")
      //.setMaster("http://127.0.0.1:8080/")
      .setAppName(this.getClass.getSimpleName)
      .set("spark.driver.allowMultipleContexts", "true")

    sparkConfig.foreach { case (k, v) => conf.setIfMissing(k, v) }

    _sc = new SparkContext(conf)
    _ss = SparkSession.builder().config(conf).getOrCreate()
  }

  def sparkConfig: Map[String, String] = Map.empty

  override def afterAll(): Unit = {
    if (_sc != null) {
      _sc.stop()
      _sc = null
    }
    super.afterAll()
  }

  def sc: SparkContext = _sc

  def ss: SparkSession = _ss

}

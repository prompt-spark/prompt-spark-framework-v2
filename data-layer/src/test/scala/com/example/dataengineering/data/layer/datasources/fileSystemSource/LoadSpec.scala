package com.example.dataengineering.data.layer.datasources.fileSystemSource

import com.example.dataengineering.data.layer.SparkSpec
import org.apache.spark.sql.Encoders
import org.scalatest.{FunSpec, GivenWhenThen, Matchers}

class LoadSpec extends FunSpec with SparkSpec with GivenWhenThen with Matchers {

  describe("USER Parquet - Spark-warehouse write and Parquet read tests") {

    it(
      "should check Parquet file column numbers and save table with metadata in spark-ware house folder") {

      val userDS = new Load[Users](
        "Users",
        "/home/abhi/Documents/own_git/spark-scala-data-engineering-framework/data-layer/src/test/scala/com/" +
          "example/dataengineering/data/layer/datasources/fileSystemSource/resources/users",
        ss,
        "overwrite",
        "/home/abhi/Desktop/testOutput/",
        true
      )(Encoders.product[Users])

      userDS.Load.columns.length shouldBe 3

    }
    it(
      "should check Parquet file column numbers and should not fail on table already present " +
        "if metadata boolean is set false") {
      val parquet1Test = new Load[Users](
        "Users",
        "/home/abhi/Documents/own_git/spark-scala-data-engineering-framework/data-layer/src/test/scala/com/" +
          "example/dataengineering/data/layer/datasources/fileSystemSource/resources/users",
        ss,
        "overwrite",
        "/home/abhi/Desktop/testOutput/",
        false
      )(Encoders.product[Users])

      parquet1Test.Load.columns.length shouldBe 3

    }
  }

  describe("USER Data Parquet - Spark-warehouse write and Parquet read tests") {

    it(
      "should check Parquet file column numbers and save table with metadata in spark-ware house folder") {
      val userDataDS = new Load[UserData](
        "UserData",
        "/home/abhi/Documents/own_git/spark-scala-data-engineering-framework/data-layer/src/test/scala/com/" +
          "example/dataengineering/data/layer/datasources/fileSystemSource/resources/userdata",
        ss,
        "overwrite",
        "/home/abhi/Desktop/testOutput/",
        true
      )(Encoders.product[UserData])

      userDataDS.Load.columns.length shouldBe 13

    }
    it(
      "should check Parquet file column numbers and should not fail on table already present " +
        "if metadata boolean is set false") {
      val userDataDS = new Load[UserData](
        "UserData",
        "/home/abhi/Documents/own_git/spark-scala-data-engineering-framework/data-layer/src/test/scala/com/" +
          "example/dataengineering/data/layer/datasources/fileSystemSource/resources/userdata",
        ss,
        "overwrite",
        "/home/abhi/Desktop/testOutput/",
        false
      )(Encoders.product[UserData])

      userDataDS.Load.columns.length shouldBe 13

    }
  }
}

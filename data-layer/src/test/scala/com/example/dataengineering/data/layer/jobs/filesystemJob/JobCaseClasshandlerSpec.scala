package com.example.dataengineering.data.layer.jobs.filesystemJob

import com.example.dataengineering.data.layer.SparkSpec
import org.scalatest.{FunSpec, GivenWhenThen, Matchers}

class JobCaseClasshandlerSpec
    extends FunSpec
    with SparkSpec
    with GivenWhenThen
    with Matchers
    with JobCaseClassHandler {
  describe("USER Parquet - Spark-warehouse write and Parquet read tests") {

    it(
      "should check Parquet file column numbers and save table with metadata in spark-ware house folder") {

      val DS = get(
        "Users",
        "/home/abhi/Documents/own_git/spark-scala-data-engineering-framework/data-layer/src/test/scala/com/" +
          "example/dataengineering/data/layer/datasources/fileSystemSource/resources/users",
        ss,
        "overwrite",
        "/home/abhi/Desktop/testOutput/",
        metadata = true
      )
      DS.columns.length shouldBe 3

    }
  }

  describe("No Table name and parquet - Should not fail") {

    it(
      "should check Parquet file column numbers and save table with metadata in spark-ware house folder") {

      val DS = get(
        "",
        "",
        ss,
        "overwrite",
        "/home/abhi/Desktop/testOutput/",
        metadata = true
      )
      DS.columns.length shouldBe 0

    }

    it("should not fail in case of table not present") {

      val DS = get(
        "dummyTableName",
        "",
        ss,
        "overwrite",
        "/home/abhi/Desktop/testOutput/",
        metadata = true
      )
      DS.columns.length shouldBe 0

    }
  }

}

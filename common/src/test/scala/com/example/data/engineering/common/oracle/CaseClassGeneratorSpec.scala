package com.example.data.engineering.common.oracle

import com.example.data.engineering.common.SparkSpec
import org.scalatest.{FunSpec, GivenWhenThen, Matchers}

class CaseClassGeneratorSpec
    extends FunSpec
    with SparkSpec
    with GivenWhenThen
    with Matchers {
  describe("Should generate case Classes") {
    it("Local HR Data Base") {
      CaseClassGenerator.writeCaseClassToFile(
        CaseClassGenerator
          .generateCaseClasses(
            ss,
            "192.168.56.103",
            1521,
            "oracle_dba",
            "oracle",
            "HR",
            "orcl",
            "com.example.dataengineering.data.layer.datasources.dbOracle"),
        "/home/abhi/Documents/own_git/" +
          "spark-scala-data-engineering-framework/common/src/test/scala/com/example/data/engineering" +
          "/common/resources/oracle/schema.scala"
      )

    }

  }
}

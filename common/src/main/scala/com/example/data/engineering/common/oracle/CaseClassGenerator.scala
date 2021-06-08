package com.example.data.engineering.common.oracle

import java.io.{BufferedWriter, FileWriter}

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

object CaseClassGenerator {

  final val SYSTEM_TABLES = "SYS.ALL_TABLES"

  def generateCaseClasses(spark: SparkSession,
                          host: String,
                          port: Int,
                          userName: String,
                          password: String,
                          schemaFilter: String,
                          serviceName: String,
                          packageName: String): Array[String] = {

    def oracleClient(tables: String) =
      spark.read
        .format("jdbc")
        .option("url",
                "jdbc:oracle:thin:@" + host + ":" + port + "/" + serviceName)
        .option("dbtable", tables)
        .option("user", userName)
        .option("password", password)
        .option("driver", "oracle.jdbc.OracleDriver")
        .load()

    val oracleDBInstance: DataFrame = oracleClient(SYSTEM_TABLES)

    val filteredTables =
      oracleDBInstance
        .filter(col("OWNER").===(schemaFilter))
        .select("TABLE_NAME")

    val allTableList: Array[String] =
      filteredTables.rdd.collect().map(row => row.mkString)

    val allTableListArray = allTableList.map { table =>
      {
        "case class " + table + "(" + oracleClient(schemaFilter + "." + table).schema.toList
          .map(x => x.name + ": String")
          .mkString(",")
      } + ") extends LoaderSchema {\n\n  override def timestamp: String = LocalDateTime.now().toString\n};\n\n"
    }

    val finalCaseClassArray = s"package $packageName \nimport java.time.LocalDateTime \nimport com.example.dataengineering.data.layer.schemas.LoaderSchema \nimport org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp \n" +: allTableListArray
    finalCaseClassArray
  }

  def writeCaseClassToFile(caseClassLists: Array[String],
                           filePath: String): Unit = {

    val writer = new BufferedWriter(new FileWriter(filePath))
    caseClassLists.foreach(writer.write)
    writer.close()
  }

}

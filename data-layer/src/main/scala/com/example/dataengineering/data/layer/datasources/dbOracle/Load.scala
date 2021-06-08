package com.example.dataengineering.data.layer.datasources.dbOracle

import com.example.dataengineering.data.layer.clients.Oracle
import com.example.dataengineering.data.layer.datasources.Loader
import com.example.dataengineering.data.layer.schemas.LoaderSchema
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

class Load[T <: LoaderSchema: Encoder](val spark: SparkSession,
                                       val host: String,
                                       val port: Int,
                                       val userName: String,
                                       val password: String,
                                       val serviceName: String,
                                       val databaseName: String,
                                       val tableName: String,
                                       val saveMode: String,
                                       val outputPath: String,
                                       val metadata: Boolean)
    extends Loader[T] {

  val dbracleInstance: Oracle[T] = new Oracle[T](spark,
                                                 host,
                                                 port,
                                                 userName,
                                                 password,
                                                 serviceName,
                                                 databaseName,
                                                 tableName,
                                                 saveMode)

  override def Load: Dataset[T] =
    dbracleInstance.provideData(metadata, outputPath).as[T]
}

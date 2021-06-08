package com.example.dataengineering.data.layer.datasources.fileSystemSource

import com.example.dataengineering.data.layer.clients.FileSystem
import com.example.dataengineering.data.layer.datasources.Loader
import com.example.dataengineering.data.layer.schemas.LoaderSchema
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

class Load[T <: LoaderSchema: Encoder](val tableName: String,
                                       val inputPath: String,
                                       val spark: SparkSession,
                                       val saveMode: String,
                                       val outputPath: String,
                                       val metadata: Boolean)
    extends Loader[T] {

  val fileSystemSourceInstance: FileSystem[T] =
    new FileSystem[T](inputPath, spark, saveMode, tableName)

  override def Load: Dataset[T] =
    fileSystemSourceInstance.provideData(metadata, outputPath).as[T]

}

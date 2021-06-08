package com.example.dataengineering.data.layer.clients

import org.apache.spark.sql.Dataset

trait DataProvider[T] {
  def provideData(metadata: Boolean, outputPath: String): Dataset[T]
}

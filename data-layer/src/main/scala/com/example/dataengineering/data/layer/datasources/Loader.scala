package com.example.dataengineering.data.layer.datasources

import org.apache.spark.sql.{DataFrame, Dataset}

trait Loader[T] {
  def Load: Dataset[T]
}

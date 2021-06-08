package com.example.dataengineering.data.layer.datasources.fileSystemSource

import java.time.LocalDateTime

import com.example.dataengineering.data.layer.schemas.LoaderSchema
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp

case class Users(
    name: String,
    favorite_color: String,
    favorite_numbers: Array[Int]
) extends LoaderSchema {

  override def timestamp: String = LocalDateTime.now().toString
}
case class UserData(
    registration_dttm: Timestamp,
    id: Integer,
    first_name: String,
    last_name: String,
    email: String,
    gender: String,
    ip_address: String,
    cc: String,
    country: String,
    birthdate: String,
    salary: Double,
    title: String,
    comments: String
) extends LoaderSchema {

  override def timestamp: String = LocalDateTime.now().toString
}

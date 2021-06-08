package com.example.dataengineering.data.layer.schemas

trait LoaderSchema {
  def status: Int = 0

  def timestamp: String
}

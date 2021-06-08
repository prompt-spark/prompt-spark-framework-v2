package com.example.dataengineering.data.layer.output
import com.example.dataengineering.data.layer.schemas.LoaderSchema
import org.apache.spark.sql.Dataset

trait Writer extends Serializable {

  def writeJson[T <: LoaderSchema](data: Dataset[T],
                                   outputPath: String): Unit = {
    data.write.mode("overwrite").option("header", "true").json(outputPath)
  }

  def writeParquet[T <: LoaderSchema](data: Dataset[T],
                                      outputPath: String): Unit = {
    data.write.mode("overwrite").option("header", "true").parquet(outputPath)
  }

  def parallelWriter[T <: LoaderSchema](data: Seq[Dataset[T]],
                                        outputPath: String,
                                        writerType: String): Unit =
    data.foreach {
      case dataset if writerType == "parquet" => writeJson(dataset, outputPath)
      case dataset if writerType == "json"    => writeParquet(dataset, outputPath)
    }

}

package com.example.dataengineering.data.layer.jobs.filesystemJob

import com.example.dataengineering.data.layer.jobs.SparkSessionForDataLayerJobs

object FileSystemBatchJob
    extends SparkSessionForDataLayerJobs
    with JobCaseClassHandler {

  private val APP_NAME = getClass.getSimpleName
  private val table_name = "table_name"
  private val input_path = "input_path"
  private val save_mode = "save_mode"
  private val output_path = "output_path"
  private val metadata = "metadata"

  case class Config(
      tableName: String = "",
      inputPath: String = "",
      saveMode: String = "",
      metadata: Boolean = false,
      outputPath: String = ""
  )

  def parseArgs(args: Array[String]): Config = {

    val parser = new scopt.OptionParser[Config](APP_NAME) {
      head(APP_NAME)

      opt[String](table_name)
        .action((x, c) => c.copy(tableName = x))
        .required()
        .text("Table Name required for folder structure and metadata service")

      opt[String](input_path)
        .action((x, c) => c.copy(inputPath = x))
        .required()
        .text("input ptah required for reading data")

      opt[String](save_mode)
        .action((x, c) => c.copy(saveMode = x))
        .required()
        .text("To decide strategy of writing options [overwrite,append]")

      opt[String](output_path)
        .action((x, c) => c.copy(outputPath = x))
        .text("OutputPath for batch job")

      opt[Boolean](metadata)
        .action((x, c) => c.copy(metadata = x))
        .text("metadata boolean required for spark-warehouse")

    }

    parser.parse(args, Config()) match {
      case Some(config) => config
      case None =>
        println("Illegal arguments.")
        sys.exit(0)
    }
  }

  def main(args: Array[String]): Unit = {
    val config = parseArgs(args)

    val tableName = config.tableName
    val inputPath = config.inputPath
    val saveMode = config.saveMode
    val outputPath = config.outputPath
    val metadata = config.metadata

    get(tableName, inputPath, newSparkSession, saveMode, outputPath, metadata)

  }

}

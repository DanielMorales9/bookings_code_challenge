package de.holidaycheck.io

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

object Loader {
  def parquet(inputPath: String)(implicit spark: SparkSession): DataFrame = {
    spark.read.parquet(inputPath)
  }

  def csv(
      filePath: String,
      header: Boolean = true,
      quote: String = null,
      schema: StructType = null
  )(implicit spark: SparkSession): DataFrame = {
    spark.read
      .option("header", header.toString)
      .option("quote", quote)
      .schema(schema)
      .csv(filePath)
  }

}

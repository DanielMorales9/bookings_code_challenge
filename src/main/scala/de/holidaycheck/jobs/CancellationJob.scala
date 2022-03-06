package de.holidaycheck.jobs

import de.holidaycheck.io.{Loader, Saver}
import de.holidaycheck.middleware.DataError
import de.holidaycheck.middleware.DataFrameOps.{
  buildPipeline,
  emptyErrorDataset
}
import de.holidaycheck.transformations.{
  AddColumnStringStage,
  AddRowKeyStage,
  CastColumnStage,
  ColumnRenamedStage,
  ParseDateTimeStringStage,
  ValidateNotNullColumnStage
}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types._

class CancellationJob(
    inputPath: String,
    outputPath: String,
    saveMode: String,
    extractionDate: String
) extends JobTemplate[(Dataset[DataError], DataFrame)] {

  implicit val spark: SparkSession = init_spark_session("Cancellations")
  implicit val rowKey: String = "rowKey"

  val inputSchema = new StructType(
    Array(
      StructField("bookingid", StringType),
      StructField("cancellation_type", IntegerType),
      StructField("enddate", StringType)
    )
  )

  def extract(): (Dataset[DataError], DataFrame) = {
    val initDf = Loader.csv(
      inputPath,
      quote = "\"",
      schema = inputSchema
    )
    (emptyErrorDataset(spark), initDf)
  }

  def transform(
      df: (Dataset[DataError], DataFrame)
  ): (Dataset[DataError], DataFrame) = {
    val cancellationPipeline = List(
      new AddRowKeyStage(),
      new ColumnRenamedStage("enddate", "end_date"),
      new ColumnRenamedStage("bookingid", "booking_id"),
      new ColumnRenamedStage("cancellation_type", "cancellation_code"),
      new ValidateNotNullColumnStage("end_date"),
      new ValidateNotNullColumnStage("cancellation_code"),
      new CastColumnStage("booking_id", "long"),
      new CastColumnStage("cancellation_code", "int"),
      new ParseDateTimeStringStage("end_date"),
      new AddColumnStringStage("extraction_date", extractionDate)
    )

    buildPipeline(cancellationPipeline, df).run
  }

  def load(df: (Dataset[DataError], DataFrame)): Unit = {
    Saver.parquet(
      df._1.withColumn("extraction_date", lit(extractionDate)),
      f"$outputPath/errors",
      saveMode,
      partitionCols = List("extraction_date")
    )
    Saver.parquet(
      df._2,
      f"$outputPath/data",
      saveMode,
      partitionCols = List("extraction_date")
    )
  }
}

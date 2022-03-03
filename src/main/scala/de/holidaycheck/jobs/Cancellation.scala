package de.holidaycheck.jobs

import de.holidaycheck.middleware.DataFrameOps.buildPipeline
import de.holidaycheck.transformations.{
  ColumnRenamedStage,
  ParseDateTimeStringStage,
  ValidateNotNullColumnStage
}
import de.holidaycheck.io.{DataLoader, DataSaver}
import org.apache.spark.sql.types.{
  IntegerType,
  LongType,
  StringType,
  StructField,
  StructType
}
import org.apache.spark.sql.{DataFrame, SparkSession}

class Cancellation(input_path: String, output_path: String, saveMode: String)
    extends Job[DataFrame] {

  implicit val spark: SparkSession = init_spark_session("Cancellations")
  implicit val rowKey: String = "booking_id"

  val inputSchema = new StructType(
    Array(
      StructField("bookingid", LongType),
      StructField("cancellation_type", IntegerType),
      StructField("enddate", StringType)
    )
  )

  def extract(): DataFrame = {
    DataLoader.csv(
      input_path,
      quote = "\"",
      schema = inputSchema
    )
  }

  def transform(df: DataFrame): DataFrame = {
    val cancellationPipeline = List(
      new ColumnRenamedStage("enddate", "end_date"),
      new ColumnRenamedStage("bookingid", "booking_id"),
      new ValidateNotNullColumnStage("end_date"),
      new ValidateNotNullColumnStage("cancellation_type"),
      new ParseDateTimeStringStage("end_date")
    )

    val (cancellationErrors, cancellationDf) =
      buildPipeline(cancellationPipeline, df).run

    cancellationDf.show()
    cancellationDf.printSchema
    cancellationErrors.show()
    cancellationDf
  }

  def load(df: DataFrame): Unit =
    DataSaver.csv(df, output_path, saveMode)
}

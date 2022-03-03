package de.holidaycheck.jobs

import de.holidaycheck.io.{DataLoader, DataSaver}
import de.holidaycheck.middleware.DataError
import de.holidaycheck.middleware.DataFrameOps.{
  buildPipeline,
  emptyErrorDataset
}
import de.holidaycheck.transformations.{
  ColumnRenamedStage,
  ParseDateTimeStringStage,
  ValidateNotNullColumnStage
}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types._

class Cancellation(input_path: String, output_path: String, saveMode: String)
    extends Job[(Dataset[DataError], DataFrame)] {

  implicit val spark: SparkSession = init_spark_session("Cancellations")
  implicit val rowKey: String = "booking_id"

  val inputSchema = new StructType(
    Array(
      StructField("bookingid", LongType),
      StructField("cancellation_type", IntegerType),
      StructField("enddate", StringType)
    )
  )

  def extract(): (Dataset[DataError], DataFrame) = {
    val initDf = DataLoader.csv(
      input_path,
      quote = "\"",
      schema = inputSchema
    )
    (emptyErrorDataset(spark), initDf)
  }

  def transform(
      df: (Dataset[DataError], DataFrame)
  ): (Dataset[DataError], DataFrame) = {
    val cancellationPipeline = List(
      new ColumnRenamedStage("enddate", "end_date"),
      new ColumnRenamedStage("bookingid", "booking_id"),
      new ValidateNotNullColumnStage("end_date"),
      new ValidateNotNullColumnStage("cancellation_type"),
      new ParseDateTimeStringStage("end_date")
    )

    buildPipeline(cancellationPipeline, df).run
  }

  def load(df: (Dataset[DataError], DataFrame)): Unit = {
    DataSaver.csv(df._1, f"$output_path/errors", saveMode)
    DataSaver.csv(df._2, f"$output_path/data", saveMode)
  }
}

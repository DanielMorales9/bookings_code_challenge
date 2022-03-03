package de.holidaycheck.jobs

import de.holidaycheck.io.{Loader, Saver}
import de.holidaycheck.middleware.DataError
import de.holidaycheck.middleware.DataFrameOps.{
  buildPipeline,
  emptyErrorDataset
}
import de.holidaycheck.transformations.{
  AddColumnStringStage,
  ColumnRenamedStage,
  ParseDateTimeStringStage,
  ValidateNotNullColumnStage
}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types._

class Cancellation(
    input_path: String,
    output_path: String,
    saveMode: String,
    extraction_date: String
) extends Job[(Dataset[DataError], DataFrame)] {

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
    val initDf = Loader.csv(
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
      new ParseDateTimeStringStage("end_date"),
      new AddColumnStringStage("extraction_date", extraction_date)
    )

    buildPipeline(cancellationPipeline, df).run
  }

  def load(df: (Dataset[DataError], DataFrame)): Unit = {
    Saver.csv(
      df._1.withColumn("extraction_date", lit(extraction_date)),
      f"$output_path/errors",
      saveMode,
      partitionCols = List("extraction_date")
    )
    Saver.csv(
      df._2,
      f"$output_path/data",
      saveMode,
      partitionCols = List("extraction_date")
    )
  }
}

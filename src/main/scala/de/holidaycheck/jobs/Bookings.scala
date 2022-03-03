package de.holidaycheck.jobs

import de.holidaycheck.middleware.DataFrameOps.{
  buildPipeline,
  emptyErrorDataset
}
import de.holidaycheck.transformations.{
  ParseDateTimeStringStage,
  ValidateAirportCodeStage,
  ValidateNotNullColumnStage
}
import de.holidaycheck.io.{DataLoader, DataSaver}
import de.holidaycheck.middleware.DataError
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types.{
  LongType,
  StringType,
  StructField,
  StructType
}

class Bookings(input_path: String, output_path: String, saveMode: String)
    extends Job[(Dataset[DataError], DataFrame)] {

  implicit val spark: SparkSession = init_spark_session("Bookings")
  implicit val rowKey: String = "booking_id"
  val inputSchema = new StructType(
    Array(
      StructField("booking_id", LongType),
      StructField("booking_date", StringType),
      StructField("arrival_date", StringType),
      StructField("departure_date", StringType),
      StructField("source", StringType),
      StructField("destination", StringType)
    )
  )

  def extract(): (Dataset[DataError], DataFrame) = {
    def initDf: DataFrame = DataLoader.csv(
      input_path,
      quote = "\"",
      schema = inputSchema
    )
    (emptyErrorDataset(spark), initDf)
  }

  def transform(
      df: (Dataset[DataError], DataFrame)
  ): (Dataset[DataError], DataFrame) = {
    val bookingPipeline = List(
      new ValidateNotNullColumnStage("booking_date"),
      new ValidateNotNullColumnStage("arrival_date"),
      new ValidateNotNullColumnStage("departure_date"),
      new ValidateNotNullColumnStage("source"),
      new ValidateNotNullColumnStage("destination"),
      new ValidateAirportCodeStage("source"),
      new ValidateAirportCodeStage("destination"),
      new ParseDateTimeStringStage("booking_date"),
      new ParseDateTimeStringStage("arrival_date"),
      new ParseDateTimeStringStage("departure_date")
    )

    buildPipeline(bookingPipeline, df).run

  }

  def load(df: (Dataset[DataError], DataFrame)): Unit = {
    val (errors, data) = df
    DataSaver.csv(data, f"$output_path/data", saveMode)
    DataSaver.csv(errors, f"$output_path/errors", saveMode)
  }

}

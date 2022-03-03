package de.holidaycheck.jobs

import de.holidaycheck.middleware.DataFrameOps.{
  buildPipeline,
  emptyErrorDataset
}
import de.holidaycheck.transformations.{
  AddColumnStringStage,
  ParseDateTimeStringStage,
  ValidateAirportCodeStage,
  ValidateNotNullColumnStage
}
import de.holidaycheck.io.{Loader, Saver}
import de.holidaycheck.middleware.DataError
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types.{
  LongType,
  StringType,
  StructField,
  StructType
}

class Bookings(
    input_path: String,
    output_path: String,
    saveMode: String,
    extraction_date: String
) extends Job[(Dataset[DataError], DataFrame)] {

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
    def initDf: DataFrame = Loader.csv(
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
      new ParseDateTimeStringStage("departure_date"),
      new AddColumnStringStage("extraction_date", extraction_date)
    )

    buildPipeline(bookingPipeline, df).run

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

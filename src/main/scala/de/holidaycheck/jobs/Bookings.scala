package de.holidaycheck.jobs

import de.holidaycheck.middleware.DataFrameOps.buildPipeline
import de.holidaycheck.transformations.{
  ParseDateTimeStringStage,
  ValidateAirportCodeStage,
  ValidateNotNullColumnStage
}
import de.holidaycheck.io.DataLoader
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{
  LongType,
  StringType,
  StructField,
  StructType
}

class Bookings(input_path: String) extends Job {

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

  def extract(): DataFrame = {
    new DataLoader(input_path)
      .csv(quote = Some("\""), schema = Some(inputSchema))
  }

  def transform(df: DataFrame): DataFrame = {
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

    val (bookingErrors, bookingDf) =
      buildPipeline(bookingPipeline, df).run

    bookingDf.show()
    bookingDf.printSchema
    bookingErrors.show()
    bookingDf
  }

  def load(df: DataFrame): DataFrame = df

}

package de.holidaycheck.jobs

import de.holidaycheck.middleware.DataFrameOps.{
  buildPipeline,
  emptyErrorDataset
}
import de.holidaycheck.transformations.{
  AddColumnStringStage,
  AddRowKeyStage,
  CastColumnStage,
  ParseDateTimeStringStage,
  ValidateAirportCodeStage,
  ValidateNotNullColumnStage
}
import de.holidaycheck.io.{Loader, Saver}
import de.holidaycheck.middleware.DataError
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class BookingsJob(
    inputPath: String,
    outputPath: String,
    saveMode: String,
    extractionDate: String
) extends JobTemplate[(Dataset[DataError], DataFrame)] {

  implicit val spark: SparkSession = init_spark_session("Bookings")
  implicit val rowKey: String = "rowKey"
  val inputSchema = new StructType(
    Array(
      StructField("booking_id", StringType),
      StructField("booking_date", StringType),
      StructField("arrival_date", StringType),
      StructField("departure_date", StringType),
      StructField("source", StringType),
      StructField("destination", StringType)
    )
  )

  def extract(): (Dataset[DataError], DataFrame) = {
    def initDf: DataFrame = Loader.csv(
      inputPath,
      quote = "\"",
      schema = inputSchema
    )
    (emptyErrorDataset(spark), initDf)
  }

  def transform(
      df: (Dataset[DataError], DataFrame)
  ): (Dataset[DataError], DataFrame) = {
    val bookingPipeline = List(
      new AddRowKeyStage(),
      new ValidateNotNullColumnStage("booking_id"),
      new ValidateNotNullColumnStage("booking_date"),
      new ValidateNotNullColumnStage("arrival_date"),
      new ValidateNotNullColumnStage("departure_date"),
      new ValidateNotNullColumnStage("source"),
      new ValidateNotNullColumnStage("destination"),
      new CastColumnStage("booking_id", "long"),
      new ValidateAirportCodeStage("source"),
      new ValidateAirportCodeStage("destination"),
      new ParseDateTimeStringStage("booking_date"),
      new ParseDateTimeStringStage("arrival_date"),
      new ParseDateTimeStringStage("departure_date"),
      new AddColumnStringStage("extraction_date", extractionDate)
    )

    buildPipeline(bookingPipeline, df).run

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

package de.holidaycheck

import cats.data.Writer
import de.holidaycheck.etl.{
  ColumnRenamedStage,
  DataLoader,
  ParseDateTimeStringStage
}
import de.holidaycheck.middleware.DataFrameOps
import de.holidaycheck.middleware.DataFrameOps._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

object SimpleApp {
  implicit val spark: SparkSession = init_spark_session

  private def runCancellationPipeline: DataFrame = {
    val cancellationSource = "cancellation.csv"

    val cancellationSchema = new StructType(
      Array(
        StructField("bookingid", LongType, nullable = false),
        StructField("cancellation_type", IntegerType, nullable = false),
        StructField("enddate", StringType, nullable = false)
      )
    )

    val initDf = new DataLoader(cancellationSource)
      .csv(quote = Some("\""), schema = Some(cancellationSchema))

    val cancellationPipeline = List(
      new ColumnRenamedStage("enddate", "end_date"),
      new ColumnRenamedStage("bookingid", "booking_id"),
      new ParseDateTimeStringStage("end_date")
    )

    val dfWithErrors = Writer(DataFrameOps.emptyErrorDataset(spark), initDf)
    val (cancellationErrors, cancellationDf) =
      buildPipeline(cancellationPipeline, dfWithErrors).run

    cancellationDf.show()
    cancellationDf.printSchema
    cancellationErrors.show()
    cancellationDf
  }

  private def runBookingPipeline: DataFrame = {
    val bookingSource = "bookings.csv"

    val bookingSchema = new StructType(
      Array(
        StructField("booking_id", LongType, nullable = false),
        StructField("booking_date", StringType, nullable = false),
        StructField("arrival_date", StringType, nullable = false),
        StructField("departure_date", StringType, nullable = false),
        StructField("source", StringType, nullable = false),
        StructField("destination", StringType, nullable = false)
      )
    )

    val initDf = new DataLoader(bookingSource)
      .csv(quote = Some("\""), schema = Some(bookingSchema))

    val bookingPipeline = List(
      new ParseDateTimeStringStage("booking_date"),
      new ParseDateTimeStringStage("arrival_date"),
      new ParseDateTimeStringStage("departure_date")
    )

    val dfWithErrors = Writer(DataFrameOps.emptyErrorDataset(spark), initDf)
    val (bookingErrors, bookingDf) =
      buildPipeline(bookingPipeline, dfWithErrors).run

    bookingDf.show()
    bookingDf.printSchema
    bookingErrors.show()
    bookingDf
  }

  private def init_spark_session: SparkSession = {
    SparkSession.builder.appName("code_challenge").getOrCreate()
  }

  def main(args: Array[String]): Unit = {
    val bookingDf: DataFrame = runBookingPipeline

    val cancellationDf: DataFrame = runCancellationPipeline

    val df = bookingDf.join(
      cancellationDf,
      usingColumns = Seq("booking_id"),
      joinType = "left"
    )

    println(bookingDf.count())
    println(df.count())

    df.show()
    df.printSchema

    spark.stop()
  }
}

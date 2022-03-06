package de.holidaycheck.jobs

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import de.holidaycheck.middleware.{DataError, DataFrameOps}
import de.holidaycheck.transformations.SparkSessionTestWrapper
import de.holidaycheck.utils.DateTimeUtils.parseDateTime
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.mockito.MockitoSugar.{mock, when}
import org.scalatest.funsuite.AnyFunSuite

class BookingsJobSpec
    extends AnyFunSuite
    with DataFrameComparer
    with SparkSessionTestWrapper {

  import spark.implicits._

  test("testing Bookings job's transformation") {
    val mockJob = mock[BookingsJob]
    when(mockJob.init_spark_session("Bookings")).thenReturn(spark)

    val job = new BookingsJob("inputPath", "outputPath", "error", "2022-01-01")

    val sourceDF = Seq(
      (
        "14723469",
        "2021-06-09 17:32:10.000",
        "2021-08-27 00:00:00.000",
        "2021-08-15 00:00:00.000",
        "CGN",
        "AYT"
      ),
      (
        "14698329",
        "2021-05-27 21:40:33.000",
        "2021-09-10 00:00:00.000",
        "2021-08-25 00:00:00.000",
        "NUE",
        "RHO"
      ),
      (
        "broken-booking-id",
        "2021-06-26 12:38:26.000",
        "2021-08-10 00:00:00.000",
        "2021-08-03 00:00:00.000",
        "MUC",
        "PMI"
      )
    ).toDF(
      "booking_id",
      "booking_date",
      "arrival_date",
      "departure_date",
      "source",
      "destination"
    )

    val inputDF = (DataFrameOps.emptyErrorDataset(spark), sourceDF)

    val expectedData = Seq(
      Row(
        14723469L,
        parseDateTime("2021-06-09 17:32:10.000"),
        parseDateTime("2021-08-27 00:00:00.000"),
        parseDateTime("2021-08-15 00:00:00.000"),
        "CGN",
        "AYT",
        "2022-01-01"
      ),
      Row(
        14698329L,
        parseDateTime("2021-05-27 21:40:33.000"),
        parseDateTime("2021-09-10 00:00:00.000"),
        parseDateTime("2021-08-25 00:00:00.000"),
        "NUE",
        "RHO",
        "2022-01-01"
      )
    )

    val (actualErrors, actualDF) = job.transform(inputDF)
    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(
        Array(
          StructField("booking_id", LongType),
          StructField("booking_date", TimestampType),
          StructField("arrival_date", TimestampType),
          StructField("departure_date", TimestampType),
          StructField("source", StringType),
          StructField("destination", StringType),
          StructField("extraction_date", StringType, nullable = false)
        )
      )
    )

    val expectedErrors = spark
      .createDataset(
        Seq(
          DataError(
            "broken-booking-id",
            "CastColumnStage",
            "booking_id",
            "broken-booking-id",
            "Unable to cast broken-booking-id to long"
          )
        )
      )

    assertSmallDataFrameEquality(actualDF, expectedDF)

    assertSmallDatasetEquality(actualErrors, expectedErrors)
  }
}

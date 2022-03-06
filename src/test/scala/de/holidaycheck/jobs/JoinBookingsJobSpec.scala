package de.holidaycheck.jobs

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import de.holidaycheck.middleware.{DataError, DataFrameOps}
import de.holidaycheck.transformations.SparkSessionTestWrapper
import de.holidaycheck.utils.DateTimeUtils.parseDateTime
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.mockito.MockitoSugar.{mock, when}
import org.scalatest.funsuite.AnyFunSuite

class JoinBookingsJobSpec
    extends AnyFunSuite
    with DataFrameComparer
    with SparkSessionTestWrapper {

  import spark.implicits._

  test("testing join job's transformation") {
    val mockJob = mock[JoinBookingsJob]
    when(mockJob.init_spark_session("JoinBookings")).thenReturn(spark)

    val job =
      new JoinBookingsJob(
        "bookingsPath",
        "cancellationPath",
        "outputPath",
        "error",
        "2022-01-01"
      )

    val sourceDF = Seq(
      (
        14723469L,
        parseDateTime("2021-06-09 17:32:10.000"),
        parseDateTime("2021-06-09 17:32:10.000"),
        parseDateTime("2021-08-27 00:00:00.000"),
        "CGN",
        "AYT",
        53,
        parseDateTime("2021-08-15 00:00:00.000")
      ),
      (
        14698329L,
        parseDateTime("2021-05-27 21:40:33.000"),
        parseDateTime("2021-09-10 00:00:00.000"),
        parseDateTime("2021-08-25 00:00:00.000"),
        "NUE",
        "RHO",
        52,
        parseDateTime("2021-05-27 21:40:33.000")
      )
    ).toDF(
      "booking_id",
      "booking_date",
      "arrival_date",
      "departure_date",
      "source",
      "destination",
      "cancellation_code",
      "end_date"
    )

    val inputDF = (DataFrameOps.emptyErrorDataset(spark), sourceDF)

    val expectedData = Seq(
      Row(
        14723469L,
        parseDateTime("2021-06-09 17:32:10.000"),
        parseDateTime("2021-06-09 17:32:10.000"),
        parseDateTime("2021-08-27 00:00:00.000"),
        "CGN",
        "AYT",
        parseDateTime("2021-08-15 00:00:00.000"),
        "cheap",
        "2022-01-01"
      ),
      Row(
        14698329L,
        parseDateTime("2021-05-27 21:40:33.000"),
        parseDateTime("2021-09-10 00:00:00.000"),
        parseDateTime("2021-08-25 00:00:00.000"),
        "NUE",
        "RHO",
        parseDateTime("2021-05-27 21:40:33.000"),
        "free",
        "2022-01-01"
      )
    )

    val (actualErrors, actualDF) = job.transform(inputDF)
    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(
        Array(
          StructField("booking_id", LongType, nullable = false),
          StructField("booking_date", TimestampType, nullable = true),
          StructField("arrival_date", TimestampType, nullable = true),
          StructField("departure_date", TimestampType, nullable = true),
          StructField("source", StringType, nullable = true),
          StructField("destination", StringType, nullable = true),
          StructField("cancellation_end_date", TimestampType, nullable = true),
          StructField("cancellation_type", StringType, nullable = false),
          StructField("extraction_date", StringType, nullable = false)
        )
      )
    )

    val expectedErrors = spark.emptyDataset[DataError]
    assertSmallDataFrameEquality(actualDF, expectedDF)

    assertSmallDatasetEquality(actualErrors, expectedErrors)
  }
}

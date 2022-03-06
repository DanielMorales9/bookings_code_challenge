package de.holidaycheck.reports

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import de.holidaycheck.transformations.SparkSessionTestWrapper
import de.holidaycheck.utils.DateTimeUtils.parseDateTime
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.mockito.MockitoSugar.{mock, when}
import org.scalatest.funsuite.AnyFunSuite

class CheapAndFreeBookingsReportSpec
    extends AnyFunSuite
    with DataFrameComparer
    with SparkSessionTestWrapper {

  import spark.implicits._

  test("testing cheap and free bookings report") {
    val mockJob = mock[CheapAndFreeBookingsReport]
    when(mockJob.init_spark_session("CheapAndFreeBookingsReport"))
      .thenReturn(spark)

    val job = new CheapAndFreeBookingsReport(
      "inputPath",
      "outputPath",
      "error"
    )

    val sourceDF = Seq(
      (
        14723469L,
        "free",
        parseDateTime("2021-06-09 17:32:10.000")
      ),
      (
        14723460L,
        "cheap",
        parseDateTime("2021-06-09 15:32:10.000")
      ),
      (
        14698329L,
        "not-cancellable",
        parseDateTime("2021-05-27 21:40:33.000")
      )
    ).toDF(
      "booking_id",
      "cancellation_type",
      "cancellation_end_date"
    )

    val inputDF = sourceDF

    val expectedData = Seq(
      Row(
        14723469L,
        "free",
        parseDateTime("2021-06-09 17:32:10.000")
      ),
      Row(
        14723460L,
        "cheap",
        parseDateTime("2021-06-09 15:32:10.000")
      )
    )

    val actualDF = job.transform(inputDF)
    val expectedDF = spark
      .createDataFrame(
        spark.sparkContext.parallelize(expectedData),
        StructType(
          Array(
            StructField("booking_id", LongType, nullable = false),
            StructField("cancellation_type", StringType),
            StructField("cancellation_end_date", TimestampType)
          )
        )
      )

    assertSmallDatasetEquality(actualDF, expectedDF)
  }
}

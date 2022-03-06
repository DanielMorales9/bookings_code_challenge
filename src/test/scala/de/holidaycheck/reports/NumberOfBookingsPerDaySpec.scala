package de.holidaycheck.reports

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import de.holidaycheck.transformations.SparkSessionTestWrapper
import de.holidaycheck.utils.DateTimeUtils.parseDateTime
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.mockito.MockitoSugar.{mock, when}
import org.scalatest.funsuite.AnyFunSuite

class NumberOfBookingsPerDaySpec
    extends AnyFunSuite
    with DataFrameComparer
    with SparkSessionTestWrapper {

  import spark.implicits._

  test("testing number of bookings per day report") {
    val mockJob = mock[NumberOfBookingsPerDay]
    when(mockJob.init_spark_session("NumberOfBookingsPerDay")).thenReturn(spark)

    val job = new NumberOfBookingsPerDay(
      "inputPath",
      "outputPath",
      "error"
    )

    val sourceDF = Seq(
      (
        14723469L,
        parseDateTime("2021-06-09 17:32:10.000")
      ),
      (
        14723460L,
        parseDateTime("2021-06-09 15:32:10.000")
      ),
      (
        14698329L,
        parseDateTime("2021-05-27 21:40:33.000")
      )
    ).toDF(
      "booking_id",
      "booking_date"
    )

    val inputDF = sourceDF

    val expectedData = Seq(
      Row(
        "2021-06-09",
        2L
      ),
      Row(
        "2021-05-27",
        1L
      )
    )

    val actualDF = job.transform(inputDF).sort("date")
    val expectedDF = spark
      .createDataFrame(
        spark.sparkContext.parallelize(expectedData),
        StructType(
          Array(
            StructField("date", StringType),
            StructField("num_bookings", LongType, nullable = false)
          )
        )
      )
      .sort("date")

    assertSmallDatasetEquality(actualDF, expectedDF)
  }
}

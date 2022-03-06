package de.holidaycheck.jobs

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import de.holidaycheck.middleware.{DataError, DataFrameOps}
import de.holidaycheck.transformations.SparkSessionTestWrapper
import de.holidaycheck.utils.DateTimeUtils.parseDateTime
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.mockito.MockitoSugar.{mock, when}
import org.scalatest.funsuite.AnyFunSuite

class CancellationSpec
    extends AnyFunSuite
    with DataFrameComparer
    with SparkSessionTestWrapper {

  import spark.implicits._

  test("testing Cancellation job's transformation") {
    val mockJob = mock[Cancellation]
    when(mockJob.init_spark_session("Cancellation")).thenReturn(spark)

    val job =
      new Cancellation("inputPath", "outputPath", "error", "2022-01-01")

    val sourceDF = Seq(
      (
        "14723469",
        "53",
        "2021-06-09 17:32:10.000"
      ),
      (
        "14698329",
        "52",
        "2021-05-27 21:40:33.000"
      ),
      (
        "14698323",
        "br",
        "2021-06-26 12:38:26.000"
      )
    ).toDF(
      "bookingid",
      "cancellation_type",
      "enddate"
    )

    val inputDF = (DataFrameOps.emptyErrorDataset(spark), sourceDF)

    val expectedData = Seq(
      Row(
        14723469L,
        53,
        parseDateTime("2021-06-09 17:32:10.000"),
        "2022-01-01"
      ),
      Row(
        14698329L,
        52,
        parseDateTime("2021-05-27 21:40:33.000"),
        "2022-01-01"
      )
    )

    val (actualErrors, actualDF) = job.transform(inputDF)
    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(
        Array(
          StructField("booking_id", LongType),
          StructField("cancellation_code", IntegerType),
          StructField("end_date", TimestampType),
          StructField("extraction_date", StringType, nullable = false)
        )
      )
    )

    val expectedErrors = spark
      .createDataset(
        Seq(
          DataError(
            "14698323",
            "CastColumnStage",
            "cancellation_code",
            "br",
            "Unable to cast br to int"
          )
        )
      )

    assertSmallDataFrameEquality(actualDF, expectedDF)

    assertSmallDatasetEquality(actualErrors, expectedErrors)
  }
}

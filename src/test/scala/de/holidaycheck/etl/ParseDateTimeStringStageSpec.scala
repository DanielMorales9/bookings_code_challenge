package de.holidaycheck.etl

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import de.holidaycheck.middleware.DataError
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{
  StringType,
  StructField,
  StructType,
  TimestampType
}
import org.scalatest.funsuite.AnyFunSuite

import java.sql.Timestamp
import java.text.SimpleDateFormat

class ParseDateTimeStringStageSpec
    extends AnyFunSuite
    with DataFrameComparer
    with SparkSessionTestWrapper {

  import spark.implicits._

  private def parseDateTime(
      dateTimeString: String,
      dateTimeFormat: String = "yyyy-MM-dd hh:mm:ss.SSS"
  ): Timestamp = {
    val dateFormat = new SimpleDateFormat(dateTimeFormat)
    val parsedDate = dateFormat.parse(dateTimeString)
    new Timestamp(parsedDate.getTime)
  }

  test("format dateTimeString to Timestamp") {

    val dateTimeString = "2021-06-26 13:38:26.000"
    val brokenDateTime = "broken_datetime"
    val sourceDF = Seq(
      ("1", dateTimeString),
      ("2", brokenDateTime)
    ).toDF("booking_id", "dateTime")

    val (actualErrors, actualDF) =
      new ParseDateTimeStringStage("dateTime").apply(sourceDF).run

    val expectedData = Seq(
      Row("1", parseDateTime(dateTimeString))
    )

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(
        List(
          StructField("booking_id", StringType, nullable = true),
          StructField("dateTime", TimestampType, nullable = true)
        )
      )
    )

    val expectedErrors = spark
      .createDataset(
        Seq(
          DataError(
            "2",
            "ParseDateTimeStringStage",
            "dateTime",
            brokenDateTime,
            "Unable to parse DateTime string"
          )
        )
      )

    assertSmallDataFrameEquality(actualDF, expectedDF)

    assertSmallDatasetEquality(actualErrors, expectedErrors)

  }

}

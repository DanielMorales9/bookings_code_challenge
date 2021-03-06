package de.holidaycheck.transformations

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import de.holidaycheck.middleware.DataError
import de.holidaycheck.utils.DateTimeUtils.parseDateTime
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{
  StringType,
  StructField,
  StructType,
  TimestampType
}
import org.scalatest.funsuite.AnyFunSuite

class ParseDateTimeStringStageSpec
    extends AnyFunSuite
    with DataFrameComparer
    with SparkSessionTestWrapper {

  import spark.implicits._

  implicit val rowKey: String = "id"

  test("format dateTimeString to Timestamp") {

    val testedColumn = "dateTime"
    val dateTimeString = "2021-06-26 13:38:26.000"
    val brokenDateTime = "broken_datetime"
    val sourceDF = Seq(
      ("1", dateTimeString),
      ("2", brokenDateTime)
    ).toDF(rowKey, testedColumn)

    val (actualErrors, actualDF) =
      new ParseDateTimeStringStage(testedColumn).apply(sourceDF).run

    val expectedData = Seq(
      Row("1", parseDateTime(dateTimeString))
    )

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(
        List(
          StructField(rowKey, StringType, nullable = true),
          StructField(testedColumn, TimestampType, nullable = true)
        )
      )
    )

    val expectedErrors = spark
      .createDataset(
        Seq(
          DataError(
            "2",
            "ParseDateTimeStringStage",
            testedColumn,
            brokenDateTime,
            "Unable to parse DateTime string"
          )
        )
      )

    assertSmallDataFrameEquality(actualDF, expectedDF)

    assertSmallDatasetEquality(actualErrors, expectedErrors)

  }

}

package de.holidaycheck.transformations

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import de.holidaycheck.middleware.DataError
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite

class ValidateAirportCodeStageSpec
    extends AnyFunSuite
    with DataFrameComparer
    with SparkSessionTestWrapper {

  import spark.implicits._
  implicit val rowKey: String = "id"

  test("validate airport code") {
    val testedColumn = "airport_code"
    val sourceDF = Seq(
      ("1", "CGN"),
      ("2", "1"),
      ("3", "cgn")
    ).toDF(rowKey, testedColumn)

    val (actualErrors, actualDF) =
      new ValidateAirportCodeStage(testedColumn)
        .apply(sourceDF)
        .run

    val expectedData = Seq(
      Row("1", "CGN")
    )

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(
        List(
          StructField(rowKey, StringType, nullable = true),
          StructField(testedColumn, StringType, nullable = true)
        )
      )
    )

    val expectedErrors = spark
      .createDataset(
        Seq(
          DataError(
            "2",
            "ValidateAirportCodeStage",
            testedColumn,
            "1",
            "Invalid Airport Code: 1"
          ),
          DataError(
            "3",
            "ValidateAirportCodeStage",
            testedColumn,
            "cgn",
            "Invalid Airport Code: cgn"
          )
        )
      )

    assertSmallDataFrameEquality(actualDF, expectedDF)

    assertSmallDatasetEquality(actualErrors, expectedErrors)

  }

}

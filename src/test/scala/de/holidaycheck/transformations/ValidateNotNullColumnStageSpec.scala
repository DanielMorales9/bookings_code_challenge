package de.holidaycheck.transformations

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import de.holidaycheck.middleware.DataError
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite

class ValidateNotNullColumnStageSpec
    extends AnyFunSuite
    with DataFrameComparer
    with SparkSessionTestWrapper {

  import spark.implicits._
  implicit val rowKey: String = "id"

  test("validate null column") {

    val testedColumn = "test_col"
    val sourceDF = Seq(
      ("1", "not_null"),
      ("2", null)
    ).toDF(rowKey, testedColumn)

    val (actualErrors, actualDF) =
      new ValidateNotNullColumnStage(testedColumn)
        .apply(sourceDF)
        .run

    val expectedData = Seq(
      Row("1", "not_null")
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
            "ValidateNotNullColumnStage",
            testedColumn,
            "null",
            "Column cannot be null"
          )
        )
      )

    assertSmallDataFrameEquality(actualDF, expectedDF)

    assertSmallDatasetEquality(actualErrors, expectedErrors)

  }

}

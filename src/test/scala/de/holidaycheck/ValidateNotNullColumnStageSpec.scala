package de.holidaycheck

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import de.holidaycheck.etl.ValidateNotNullColumnStage
import de.holidaycheck.middleware.DataError
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite

class ValidateNotNullColumnStageSpec
    extends AnyFunSuite
    with DataFrameComparer
    with SparkSessionTestWrapper {

  import spark.implicits._

  test("validate null column") {

    val sourceDF = Seq(
      ("1", "not_null"),
      ("2", null)
    ).toDF("booking_id", "test_col")

    val (actualErrors, actualDF) =
      new ValidateNotNullColumnStage("test_col").apply(sourceDF).run

    val expectedData = Seq(
      Row("1", "not_null")
    )

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(
        List(
          StructField("booking_id", StringType, nullable = true),
          StructField("test_col", StringType, nullable = true)
        )
      )
    )

    val expectedErrors = spark
      .createDataset(
        Seq(
          DataError(
            "2",
            "ValidateNotNullColumnStage",
            "test_col",
            "null",
            "Column cannot be null"
          )
        )
      )
      .sort("rowKey")

    assertSmallDataFrameEquality(actualDF, expectedDF)

    assertSmallDatasetEquality(actualErrors.sort("rowKey"), expectedErrors)

  }

}

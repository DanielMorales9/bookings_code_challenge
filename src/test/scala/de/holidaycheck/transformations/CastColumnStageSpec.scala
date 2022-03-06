package de.holidaycheck.transformations

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import de.holidaycheck.middleware.DataError
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{
  IntegerType,
  LongType,
  StringType,
  StructField,
  StructType
}
import org.scalatest.funsuite.AnyFunSuite

class CastColumnStageSpec
    extends AnyFunSuite
    with DataFrameComparer
    with SparkSessionTestWrapper {

  import spark.implicits._

  implicit val rowKey: String = "id"

  test("cast to int") {

    val testedColumn = "test_column"
    val sourceDF = Seq(
      ("1", "13"),
      ("2", "broken")
    ).toDF(rowKey, testedColumn)

    val (actualErrors, actualDF) =
      new CastColumnStage(testedColumn, "int").apply(sourceDF).run

    val expectedData = Seq(
      Row("1", 13)
    )

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(
        List(
          StructField(rowKey, StringType, nullable = true),
          StructField(testedColumn, IntegerType, nullable = true)
        )
      )
    )

    val expectedErrors = spark
      .createDataset(
        Seq(
          DataError(
            "2",
            "CastColumnStage",
            testedColumn,
            "broken",
            f"Unable to cast broken to int"
          )
        )
      )

    assertSmallDataFrameEquality(actualDF, expectedDF)

    assertSmallDatasetEquality(actualErrors, expectedErrors)

  }

  test("cast to long") {

    val testedColumn = "test_column"
    val sourceDF = Seq(
      ("1", "13"),
      ("2", "broken")
    ).toDF(rowKey, testedColumn)

    val (actualErrors, actualDF) =
      new CastColumnStage(testedColumn, "long").apply(sourceDF).run

    val expectedData = Seq(
      Row("1", 13L)
    )

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(
        List(
          StructField(rowKey, StringType, nullable = true),
          StructField(testedColumn, LongType, nullable = true)
        )
      )
    )

    val expectedErrors = spark
      .createDataset(
        Seq(
          DataError(
            "2",
            "CastColumnStage",
            testedColumn,
            "broken",
            f"Unable to cast broken to long"
          )
        )
      )

    assertSmallDataFrameEquality(actualDF, expectedDF)

    assertSmallDatasetEquality(actualErrors, expectedErrors)

  }

}

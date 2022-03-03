package de.holidaycheck.transformations

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import de.holidaycheck.middleware.DataError
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite

class ColumnRenamedStageSpec
    extends AnyFunSuite
    with DataFrameComparer
    with SparkSessionTestWrapper {

  import spark.implicits._

  test("column renamed") {
    val testedColumn = "_id"
    val sourceDF = Seq(
      "1",
      "2",
      "3"
    ).toDF(testedColumn)

    val renamedColumn = "id"
    val (actualErrors, actualDF) =
      new ColumnRenamedStage(testedColumn, renamedColumn)
        .apply(sourceDF)
        .run

    val expectedData = Seq(
      Row("1"),
      Row("2"),
      Row("3")
    )

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(
        List(
          StructField(renamedColumn, StringType, nullable = true)
        )
      )
    )

    val expectedErrors = spark.emptyDataset[DataError]

    assertSmallDataFrameEquality(actualDF, expectedDF)

    assertSmallDatasetEquality(actualErrors, expectedErrors)

  }

}

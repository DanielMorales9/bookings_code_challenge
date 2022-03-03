package de.holidaycheck.transformations

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import de.holidaycheck.middleware.DataError
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite

class AddColumnStringStageSpec
    extends AnyFunSuite
    with DataFrameComparer
    with SparkSessionTestWrapper {

  import spark.implicits._

  test("add column literal") {
    val id = "id"
    val sourceDF = Seq(
      "1",
      "2",
      "3"
    ).toDF(id)

    val comment = "true"
    val testedColumn = "comment"
    val (actualErrors, actualDF) =
      new AddColumnStringStage(testedColumn, comment)
        .apply(sourceDF)
        .run

    val expectedData = Seq(
      Row("1", comment),
      Row("2", comment),
      Row("3", comment)
    )

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(
        List(
          StructField(id, StringType, nullable = true),
          StructField(testedColumn, StringType, nullable = false)
        )
      )
    )

    val expectedErrors = spark.emptyDataset[DataError]

    assertSmallDataFrameEquality(actualDF, expectedDF)

    assertSmallDatasetEquality(actualErrors, expectedErrors)

  }

}

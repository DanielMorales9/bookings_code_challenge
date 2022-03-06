package de.holidaycheck.transformations

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import de.holidaycheck.jobs.BookingsJob
import de.holidaycheck.middleware.DataError
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{
  LongType,
  StringType,
  StructField,
  StructType
}
import org.mockito.MockitoSugar.{mock, when}
import org.scalatest.funsuite.AnyFunSuite

class AddRowKeyStageSpec
    extends AnyFunSuite
    with DataFrameComparer
    with SparkSessionTestWrapper {

  import spark.implicits._
  implicit val rowKey: String = "rowKey"

  test("add row key") {
    val id = "id"
    val sourceDF = Seq(
      "1",
      "2",
      "3"
    ).toDF(id)

    val comment = "true"
    val testedColumn = "rowKey"
    val (actualErrors, actualDF) =
      new AddRowKeyStage().apply(sourceDF).run

    val expectedData = Seq(
      Row("1", 0L),
      Row("2", 1L),
      Row("3", 2L)
    )

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(
        List(
          StructField(id, StringType, nullable = true),
          StructField(testedColumn, LongType, nullable = false)
        )
      )
    )

    val expectedErrors = spark.emptyDataset[DataError]

    assertSmallDataFrameEquality(actualDF, expectedDF)

    assertSmallDatasetEquality(actualErrors, expectedErrors)

  }

}

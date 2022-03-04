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

class QualifyCancellationCodeStageSpec
    extends AnyFunSuite
    with DataFrameComparer
    with SparkSessionTestWrapper {

  import spark.implicits._

  test("qualify cancellation codes") {
    val cancellationCode = "cancellation_code"
    val cancellationType = "cancellation_type"
    val sourceDF = Seq(
      52,
      53,
      56
    ).toDF(cancellationCode)

    val (actualErrors, actualDF) =
      new QualifyCancellationCodeStage()
        .apply(sourceDF)
        .run

    val expectedData = Seq(
      Row("free"),
      Row("cheap"),
      Row("unknown")
    )

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(
        List(
          StructField(cancellationType, StringType, nullable = false)
        )
      )
    )

    val expectedErrors = spark.emptyDataset[DataError]
    assertSmallDataFrameEquality(actualDF, expectedDF)

    assertSmallDatasetEquality(actualErrors, expectedErrors)
  }
}

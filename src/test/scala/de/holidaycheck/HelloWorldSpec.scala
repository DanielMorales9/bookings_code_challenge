package de.holidaycheck

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite

class HelloWorldSpec extends AnyFunSuite with DataFrameComparer with SparkSessionTestWrapper {

  import spark.implicits._

  test("appends a greeting column to a Dataframe") {

    val sourceDF = Seq(
      "miguel",
      "luisa"
    ).toDF("name")

    val actualDF = sourceDF.transform(HelloWorld.withGreeting())

    val expectedSchema = List(
      StructField("name", StringType, nullable = true),
      StructField("greeting", StringType, nullable = false)
    )

    val expectedData = Seq(
      Row("miguel", "hello world"),
      Row("luisa", "hello world")
    )

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(expectedSchema)
    )

    assertSmallDataFrameEquality(actualDF, expectedDF)

  }

}
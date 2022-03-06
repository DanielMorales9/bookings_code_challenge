package de.holidaycheck.jobs

import de.holidaycheck.io.{Loader, Saver}
import de.holidaycheck.middleware.DataError
import de.holidaycheck.middleware.DataFrameOps.{
  buildPipeline,
  emptyErrorDataset
}
import de.holidaycheck.transformations.{
  AddColumnStringStage,
  ColumnRenamedStage,
  QualifyCancellationCodeStage
}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

class JoinBookings(
    bookingsPath: String,
    cancellationPath: String,
    outputPath: String,
    saveMode: String,
    extractionDate: String
) extends JobTemplate[(Dataset[DataError], DataFrame)] {
  implicit val spark: SparkSession = init_spark_session("JoinBookings")

  def extract(): (Dataset[DataError], DataFrame) = {
    val bookingDF = Loader.parquet(bookingsPath)
    val cancellationDF = Loader.parquet(cancellationPath)
    (
      emptyErrorDataset(spark),
      bookingDF.join(
        cancellationDF,
        usingColumns = Seq("booking_id"),
        joinType = "left"
      )
    )

  }

  def transform(
      df: (Dataset[DataError], DataFrame)
  ): (Dataset[DataError], DataFrame) = {

    val pipeline = List(
      new QualifyCancellationCodeStage(),
      new ColumnRenamedStage("end_date", "cancellation_end_date"),
      new AddColumnStringStage("extraction_date", extractionDate)
    )
    buildPipeline(pipeline, df).run

  }

  def load(df: (Dataset[DataError], DataFrame)): Unit = {
    Saver.parquet(
      df._1.withColumn("extraction_date", lit(extractionDate)),
      f"$outputPath/errors",
      saveMode,
      partitionCols = List("extraction_date")
    )
    Saver.parquet(
      df._2,
      f"$outputPath/data",
      saveMode,
      partitionCols = List("extraction_date")
    )
  }
}

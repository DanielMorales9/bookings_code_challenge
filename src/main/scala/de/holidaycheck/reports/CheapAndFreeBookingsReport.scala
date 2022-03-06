package de.holidaycheck.reports

import de.holidaycheck.io.{Loader, Saver}
import de.holidaycheck.jobs.JobTemplate
import org.apache.spark.sql.functions.{col, date_format}
import org.apache.spark.sql.{DataFrame, SparkSession}

class CheapAndFreeBookingsReport(
    inputPath: String,
    outputPath: String,
    saveMode: String
) extends JobTemplate[DataFrame] {
  override implicit val spark: SparkSession = init_spark_session(
    "CheapAndFreeBookingsReport"
  )

  override def extract(): DataFrame = {
    Loader.parquet(inputPath)
  }

  override def transform(df: DataFrame): DataFrame = {
    df.where(col("cancellation_type").isin("free", "cheap"))
      .select("booking_id", "cancellation_type", "cancellation_end_date")
  }
  override def load(df: DataFrame): Unit = {
    Saver.csv(df, outputPath, saveMode)
  }
}

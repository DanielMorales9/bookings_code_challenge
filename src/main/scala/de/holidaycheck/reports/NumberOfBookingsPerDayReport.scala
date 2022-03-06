package de.holidaycheck.reports

import de.holidaycheck.io.{Loader, Saver}
import de.holidaycheck.jobs.JobTemplate
import org.apache.spark.sql.functions.{col, date_format}
import org.apache.spark.sql.{DataFrame, SparkSession}

class NumberOfBookingsPerDayReport(
    inputPath: String,
    outputPath: String,
    saveMode: String
) extends JobTemplate[DataFrame] {
  override implicit val spark: SparkSession = init_spark_session(
    "NumberOfBookingsPerDay"
  )

  override def extract(): DataFrame = {
    Loader.parquet(inputPath)
  }

  override def transform(df: DataFrame): DataFrame = {
    df.select(
      date_format(col("booking_date"), "yyyy-MM-dd").alias("date")
    ).groupBy("date")
      .count()
      .select(col("date"), col("count").alias("num_bookings"))
  }
  override def load(df: DataFrame): Unit = {
    Saver.csv(df, outputPath, saveMode)
  }
}

package de.holidaycheck.etl

import cats.data.Writer
import de.holidaycheck.middleware.{DataFrameOps, DataStage}
import org.apache.spark.sql.functions.{col, to_timestamp}
import org.apache.spark.sql.{DataFrame, SparkSession}

class DateTimeFormatStage(column: String)(implicit spark: SparkSession) extends DataStage[DataFrame] {

  override val stage: String = getClass.getSimpleName

  def apply(data: DataFrame): DataSetWithErrors[DataFrame] = formatDateTime(data)

  def formatDateTime(data: DataFrame): DataSetWithErrors[DataFrame] = {
    Writer(DataFrameOps.emptyErrorDataset(spark), data.withColumn(column, to_timestamp(col(column))))
  }
}
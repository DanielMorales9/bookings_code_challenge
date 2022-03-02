package experiment

import org.apache.spark.sql.functions.{col, to_timestamp}
import org.apache.spark.sql.{DataFrame, SparkSession}

class DateTimeFormatStage(column: String)(implicit spark: SparkSession) extends DataStage[DataFrame] {

  override val stage: String = getClass.getSimpleName

  def apply(data: DataFrame): DataFrame = formatDateTime(data)

  def formatDateTime(data: DataFrame): DataFrame = {
    data.withColumn(column, to_timestamp(col(column)))
  }
}
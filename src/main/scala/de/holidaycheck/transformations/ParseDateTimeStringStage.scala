package de.holidaycheck.transformations

import cats.data.Writer
import de.holidaycheck.middleware.{DataError, DataStage}
import org.apache.spark.sql.functions.{col, to_timestamp}
import org.apache.spark.sql.{DataFrame, SparkSession}

class ParseDateTimeStringStage(columnName: String)(implicit
    spark: SparkSession,
    rowKey: String
) extends DataStage[DataFrame] {

  override val stage: String = getClass.getSimpleName

  def apply(data: DataFrame): DataSetWithErrors[DataFrame] = formatDateTime(
    data
  )

  def formatDateTime(data: DataFrame): DataSetWithErrors[DataFrame] = {

    import spark.implicits._

    val errors = data
      .filter(to_timestamp(col(columnName)).isNull)
      .select(rowKey, columnName)
      .map(row => {
        var dateTimeColumn = row(1)
        if (dateTimeColumn == null) dateTimeColumn = "null"

        DataError(
          row(0).toString,
          stage,
          columnName,
          dateTimeColumn.toString,
          "Unable to parse DateTime string"
        )
      })

    val validData = data
      .withColumn(columnName, to_timestamp(col(columnName)))
      .filter(col(columnName).isNotNull)
    Writer(errors, validData)
  }
}

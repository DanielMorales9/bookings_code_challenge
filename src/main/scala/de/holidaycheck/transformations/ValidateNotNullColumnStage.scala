package de.holidaycheck.transformations

import cats.data.Writer
import de.holidaycheck.middleware.{DataError, DataStage}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

class ValidateNotNullColumnStage(columnName: String)(implicit
    spark: SparkSession,
    rowKey: String
) extends DataStage[DataFrame] {

  override val stage: String = getClass.getSimpleName

  def apply(data: DataFrame): DataSetWithErrors[DataFrame] = validateNonNull(
    data
  )

  def validateNonNull(data: DataFrame): DataSetWithErrors[DataFrame] = {
    import spark.implicits._

    val errors = data
      .filter(col(columnName).isNull)
      .select(rowKey, columnName)
      .map(row =>
        DataError(
          row(0).toString,
          stage,
          columnName,
          "null",
          "Column cannot be null"
        )
      )

    val validData = data.filter(col(columnName).isNotNull)

    Writer(errors, validData)
  }
}

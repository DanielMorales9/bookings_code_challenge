package de.holidaycheck.transformations

import cats.data.Writer
import de.holidaycheck.middleware.{DataError, DataFrameOps, DataStage}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

class ValidateAirportCodeStage(columnName: String)(implicit
    spark: SparkSession,
    rowKey: String
) extends DataStage[DataFrame] {

  override val stage: String = getClass.getSimpleName

  def apply(data: DataFrame): DataSetWithErrors[DataFrame] = renameColumn(data)

  def renameColumn(data: DataFrame): DataSetWithErrors[DataFrame] = {
    import spark.implicits._

    val errors = data
      .filter(!col(columnName).rlike("^[A-Z]{3}$"))
      .select(rowKey, columnName)
      .map(row => {
        val airportCode = row(1).toString
        DataError(
          row(0).toString,
          stage,
          columnName,
          airportCode,
          f"Invalid Airport Code: $airportCode"
        )
      })
    Writer(
      errors,
      data.filter(col(columnName).rlike("^[A-Z]{3}$"))
    )
  }

}

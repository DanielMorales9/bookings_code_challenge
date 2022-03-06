package de.holidaycheck.transformations

import cats.data.Writer
import de.holidaycheck.middleware.{DataError, DataStage}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, SparkSession}

class CastColumnStage(columnName: String, castType: String)(implicit
    spark: SparkSession,
    rowKey: String
) extends DataStage[DataFrame] {

  override val stage: String = getClass.getSimpleName

  def apply(data: DataFrame): DataSetWithErrors[DataFrame] = castColumn(data)

  def castColumn(data: DataFrame): DataSetWithErrors[DataFrame] = {

    import spark.implicits._

    val errors = data
      .filter(col(columnName).cast(castType).isNull)
      .select(rowKey, columnName)
      .map(row => {
        val value = row(1).toString
        DataError(
          row(0).toString,
          stage,
          columnName,
          value,
          f"Unable to cast $value to $castType"
        )
      })

    Writer(
      errors,
      data
        .withColumn(columnName, col(columnName).cast(castType))
        .filter(col(columnName).isNotNull)
    )
  }

}

package de.holidaycheck.transformations

import cats.data.Writer
import de.holidaycheck.middleware.{DataFrameOps, DataStage}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}

class AddColumnStringStage(columnName: String, literal: String)(implicit
    spark: SparkSession
) extends DataStage[DataFrame] {

  override val stage: String = getClass.getSimpleName

  def apply(data: DataFrame): DataSetWithErrors[DataFrame] = renameColumn(data)

  def renameColumn(data: DataFrame): DataSetWithErrors[DataFrame] = {
    Writer(
      DataFrameOps.emptyErrorDataset(spark),
      data.withColumn(columnName, lit(literal))
    )
  }

}

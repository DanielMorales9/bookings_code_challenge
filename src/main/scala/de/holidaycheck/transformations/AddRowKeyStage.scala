package de.holidaycheck.transformations

import cats.data.Writer
import de.holidaycheck.middleware.{DataFrameOps, DataStage}
import org.apache.spark.sql.functions.{lit, monotonically_increasing_id, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}

class AddRowKeyStage(implicit spark: SparkSession, rowKey: String)
    extends DataStage[DataFrame] {

  val stage: String = getClass.getSimpleName

//  def generateUUID(): String = java.util.UUID.randomUUID().toString

  def apply(df: DataFrame): DataSetWithErrors[DataFrame] = {
    /*
    We could use a random UUID
     instead of the monotonically increasing function for simplicity
     */
    // val uuid = udf(() => generateUUID())

    Writer(
      DataFrameOps.emptyErrorDataset(spark),
      df.withColumn(rowKey, monotonically_increasing_id())
    )
  }
}

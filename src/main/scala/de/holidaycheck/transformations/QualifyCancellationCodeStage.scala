package de.holidaycheck.transformations

import cats.data.Writer
import de.holidaycheck.middleware.{DataFrameOps, DataStage}
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

class QualifyCancellationCodeStage(implicit
    spark: SparkSession
) extends DataStage[DataFrame] {
  override def apply(dataRecords: DataFrame): DataSetWithErrors[DataFrame] = {
    val cancellationCode = "cancellation_code"
    val enrichedDF = dataRecords
      .withColumn(
        "cancellation_type",
        when(col(cancellationCode) === 52, "free")
          .when(col(cancellationCode) === 53, "cheap")
          .otherwise("not-cancellable")
      )
      .drop("cancellation_code")
    Writer(DataFrameOps.emptyErrorDataset(spark), enrichedDF)
  }

  override val stage: String = getClass.getSimpleName
}

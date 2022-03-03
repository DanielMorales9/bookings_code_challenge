package experiment

import cats.data.Writer
import org.apache.spark.sql.{DataFrame, SparkSession}

class ColumnRenamedStage(column: String, columnRenamed: String)(implicit spark: SparkSession) extends DataStage[DataFrame] {

  override val stage: String = getClass.getSimpleName

  def apply(data: DataFrame): DataSetWithErrors[DataFrame] = renameColumn(data)

  def renameColumn(data: DataFrame): DataSetWithErrors[DataFrame] = {
    Writer(DataFrameOps.emptyErrorDataset(spark), data.withColumnRenamed(column, columnRenamed))
  }

}

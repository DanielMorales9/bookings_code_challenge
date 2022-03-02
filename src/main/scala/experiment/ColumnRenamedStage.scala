package experiment

import org.apache.spark.sql.{DataFrame, SparkSession}

class ColumnRenamedStage(column: String, columnRenamed: String)(implicit spark: SparkSession) extends DataStage[DataFrame] {

  override val stage: String = getClass.getSimpleName

  def apply(data: DataFrame): DataFrame = renameColumn(data)

  def renameColumn(data: DataFrame): DataFrame = {
    data.withColumnRenamed(column, columnRenamed)
  }
}

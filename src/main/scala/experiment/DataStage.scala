package experiment

import org.apache.spark.sql.DataFrame

trait DataStage[T] extends Serializable {

  def apply(dataRecords: T): DataFrame

  def stage: String
}
package de.holidaycheck.middleware

import cats.data.Writer
import org.apache.spark.sql.Dataset

trait DataStage[T <: Dataset[_]] extends Serializable {
  type DataSetWithErrors[A] = Writer[Dataset[DataError], A]

  def apply(dataRecords: T): DataSetWithErrors[T]

  def stage: String
}

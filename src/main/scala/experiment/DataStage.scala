package experiment

import org.apache.spark.sql.Dataset
import scala.language.higherKinds
import cats.data.Writer
trait DataStage[T <: Dataset[_]] extends Serializable {
  type DataSetWithErrors[A] = Writer[Dataset[DataError], A]

  def apply(dataRecords: T): DataSetWithErrors[T]

  def stage: String
}
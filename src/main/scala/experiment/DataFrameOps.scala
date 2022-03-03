package experiment

import cats.Semigroup
import experiment.SimpleApp.spark
import org.apache.spark.sql.{Dataset, SparkSession}
import spark.implicits._
object DataFrameOps {

  val emptyErrorDataset: SparkSession => Dataset[DataError] = (spark:SparkSession) => {
    spark.emptyDataset[DataError]
  }

  implicit val dataFrameSemigroup: Semigroup[Dataset[DataError]] = (x: Dataset[DataError], y: Dataset[DataError]) => x.union(y)


}
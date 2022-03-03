package de.holidaycheck.middleware

import cats.Semigroup
import org.apache.spark.sql.{Dataset, SparkSession}

object DataFrameOps {

  val emptyErrorDataset: SparkSession => Dataset[DataError] = (spark: SparkSession) => {
    import spark.implicits._
    spark.emptyDataset[DataError]
  }

  implicit val dataFrameSemigroup: Semigroup[Dataset[DataError]] = (x: Dataset[DataError], y: Dataset[DataError]) => x.union(y)


}

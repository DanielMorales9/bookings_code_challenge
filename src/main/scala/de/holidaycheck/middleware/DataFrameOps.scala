package de.holidaycheck.middleware

import cats.{Id, Semigroup}
import cats.data.WriterT
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object DataFrameOps {

  val emptyErrorDataset: SparkSession => Dataset[DataError] =
    (spark: SparkSession) => {
      import spark.implicits._

      spark.emptyDataset[DataError]
    }

  implicit val dataFrameSemigroup: Semigroup[Dataset[DataError]] =
    (x: Dataset[DataError], y: Dataset[DataError]) => x.union(y)

  def buildPipeline(
      pipeline: List[DataStage[DataFrame]],
      df: WriterT[Id, Dataset[DataError], DataFrame]
  ): WriterT[Id, Dataset[DataError], DataFrame] = {
    pipeline.foldLeft(df) { case (dfWithErrors, stage) =>
      for {
        df <- dfWithErrors
        applied <- stage.apply(df)
      } yield applied
    }
  }

}

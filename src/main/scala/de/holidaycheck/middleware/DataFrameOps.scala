package de.holidaycheck.middleware

import cats.data.{Writer, WriterT}
import cats.{Id, Semigroup}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object DataFrameOps {

  implicit val dataFrameSemigroup: Semigroup[Dataset[DataError]] =
    (x: Dataset[DataError], y: Dataset[DataError]) => x.union(y)

  def buildPipeline(
      pipeline: List[DataStage[DataFrame]],
      initDf: DataFrame
  )(implicit
      spark: SparkSession
  ): WriterT[Id, Dataset[DataError], DataFrame] = {

    val df = Writer(emptyErrorDataset(spark), initDf)
    pipeline.foldLeft(df) { case (dfWithErrors, stage) =>
      for {
        df <- dfWithErrors
        applied <- stage.apply(df)
      } yield applied
    }
  }

  def emptyErrorDataset(spark: SparkSession): Dataset[DataError] = {
    import spark.implicits._

    spark.emptyDataset[DataError]
  }
}

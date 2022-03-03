package de.holidaycheck.middleware

import cats.{Id, Semigroup}
import cats.data.{Writer, WriterT}
import de.holidaycheck.SimpleApp.spark
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
      initDf: DataFrame
  ): WriterT[Id, Dataset[DataError], DataFrame] = {

    val df = Writer(DataFrameOps.emptyErrorDataset(spark), initDf)
    pipeline.foldLeft(df) { case (dfWithErrors, stage) =>
      for {
        df <- dfWithErrors
        applied <- stage.apply(df)
      } yield applied
    }
  }

}

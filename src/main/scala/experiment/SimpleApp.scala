package experiment

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

object SimpleApp {
  val bookingFile = "bookings.csv"
  val cancellationFile = "cancellation.csv"

  val cancellationSchema = new StructType(
    Array(
      StructField("bookingid", LongType, nullable = false),
      StructField("cancellation_type", IntegerType, nullable = false),
      StructField("enddate", StringType, nullable = false)
    )
  )

  val bookingSchema = new StructType(
    Array(
      StructField("booking_id", LongType, nullable = false),
      StructField("booking_date", StringType, nullable = false),
      StructField("arrival_date", StringType, nullable = false),
      StructField("departure_date", StringType, nullable = false),
      StructField("source", StringType, nullable = false),
      StructField("destination", StringType, nullable = false))
  )

  implicit val spark: SparkSession = init_spark_session

  private def init_spark_session: SparkSession = {
    SparkSession.builder.appName("Bookings").getOrCreate()
  }

  def main(args: Array[String]): Unit = {

    val initDf = new DataLoader(bookingFile).csv(quote = Some("\""), schema = Some(bookingSchema))

    val bookingPipeline = List(
      new DateTimeFormatStage("booking_date"),
      new DateTimeFormatStage("arrival_date"),
      new DateTimeFormatStage("departure_date"),
    )

    val bookingDf = runPipeline(initDf, bookingPipeline)

    bookingDf.show()
    bookingDf.printSchema

    var cancellationDf = new DataLoader(cancellationFile).csv(quote = Some("\""), schema = Some(cancellationSchema))

    val cancellationPipeline = List(
      new ColumnRenamedStage("enddate", "end_date"),
      new ColumnRenamedStage("bookingid", "booking_id"),
      new DateTimeFormatStage("end_date")
    )

    cancellationDf = runPipeline(cancellationDf, cancellationPipeline)

    cancellationDf.show()
    cancellationDf.printSchema

    val df = bookingDf.join(cancellationDf, usingColumns = Seq("booking_id"), joinType = "left")

    println(bookingDf.count())
    println(df.count())

    df.show()
    df.printSchema

    spark.stop()
  }

  private def runPipeline(initDf: DataFrame, pipeline: List[DataStage[DataFrame]]): DataFrame = {
    pipeline.foldLeft(initDf) {
      case (df, stage) => stage(df)
    }
  }
}

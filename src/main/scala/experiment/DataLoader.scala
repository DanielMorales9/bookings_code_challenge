package experiment

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType

class DataLoader(filePath: String)(implicit spark: SparkSession) {

  def csv(header: Boolean = true, quote: Option[String], schema: Option[StructType]): DataFrame = {
    spark.read
    .option("header", header.toString)
    .option("quote", quote.orNull)
    .schema(schema.orNull)
    .csv(filePath)
  }

}

package nl.getthere.dataunit

import nl.getthere.dataunit.iolink.Distance
import org.apache.spark.eventhubs.{EventHubsConf, EventPosition}
import org.apache.spark.sql.functions.{col, from_json, from_unixtime, udf}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object IoLinkReader {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("iolink")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val connectionString = sys.env.getOrElse("EVENT_HUB", throw new Exception("EVENT_HUB not set."))

    val conf =
      EventHubsConf(connectionString)
        .setStartingPosition(EventPosition.fromEndOfStream)
        .toMap

    val schema = new StructType()
      .add("time", LongType)
      .add("port", IntegerType)
      .add("data", StringType)

    val dist_udf = udf((data: String) => Distance.extract(data))

    spark
      .readStream
      .format("eventhubs")
      .options(conf)
      .load

      .select(from_json(col("body").cast(StringType), schema).as("col"))
      .select("col.*")
      .withColumn("distance", iolink.Distance.getUdf(col("data")))
      .withColumn("timestamp", from_unixtime(col("time") / 1000))
      .drop("time")
      .drop("data")

      .writeStream
      .foreachBatch((batch: Dataset[Row], _: Long) => {
        batch.show(false)
      })
      .start
      .awaitTermination
  }

}

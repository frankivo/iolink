package nl.getthere.dataunit

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

    val dist_udf = udf((data: String) => distance(data))

    spark
      .readStream
      .format("eventhubs")
      .options(conf)
      .load

      .select(from_json(col("body").cast(StringType), schema).as("col"))
      .select("col.*")
      .withColumn("distance", dist_udf(col("data")))
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

  def distance(hex: String): Int = {
    val i = Integer.parseInt(hex, 16)
    val bin = i.toBinaryString
    val pad = bin.reverse.padTo(12, "0").mkString.reverse
    val sub = pad.slice(0, 8)
    Integer.parseInt(sub.toString, 2)
  }

}

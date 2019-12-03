package nl.getthere.dataunit.iolink

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

// Converts hex-data of O5D150 to distance in centimeters.
object Distance extends Hextract {
  override def extract(hex: String): Int = Hextract.extract(hex, 12, 4)

  override def getUdf: UserDefinedFunction = udf((data: String) => extract(data))
}

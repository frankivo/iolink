package nl.getthere.dataunit.iolink
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

// Converts hex-data of PI2794 to pressure in bar.
object Pressure extends Hextract {
  override def extract(hex: String): Double = Hextract.extract(hex, 14, 2) * 0.01

  override def getUdf: UserDefinedFunction = udf((data: String) => extract(data))
}

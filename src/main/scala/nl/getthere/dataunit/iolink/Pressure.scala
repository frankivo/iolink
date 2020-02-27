package nl.getthere.dataunit.iolink

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import scala.math.BigDecimal.RoundingMode

// Converts hex-data of PI2794 to pressure in bar.
object Pressure extends Hextract {
  def round(value: Double, decimals: Int = 2): Double =
    BigDecimal.apply(value).setScale(decimals, RoundingMode.HALF_UP).toDouble

  override def extract(hex: String): Double = round(Hextract.extract(hex, 14, 2) * 0.01)

  override def getUdf: UserDefinedFunction = udf((data: String) => extract(data))
}

package nl.getthere.dataunit.iolink

import org.apache.spark.sql.expressions.UserDefinedFunction

trait Hextract {
  // Get data
  def extract(hex: String): Any

  // Get spark udf
  def getUdf: UserDefinedFunction
}

// Converts hex data to int
object Hextract {
  def extract(hex: String, bits: Int, offset: Int): Int = {
    val i = Integer.parseInt(hex, 16)
    val bin = i.toBinaryString
    val pad = bin.reverse.padTo(bits, "0").mkString.reverse
    val sub = pad.slice(0, bits - offset)
    Integer.parseInt(sub.toString, 2)
  }
}


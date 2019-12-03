package nl.getthere.dataunit.iolink

// Converts hex-data of O5D150 to distance in centimeters.
object Distance extends Hextract {
  def extract(hex: String) : Int = Hextract.extract(hex, 12, 4)
}

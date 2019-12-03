package nl.getthere.dataunit.iolink

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class DistanceTest {
  @Test
  def testDistance29(): Unit = {
    val hex = "01D1"
    val expected = 29
    assertEquals(expected, Distance.extract(hex))
  }

  @Test
  def testDistanceIntOverflow(): Unit = {
    val hex = "0411"
    val expected = 65
    assertEquals(expected, Distance.extract(hex))
  }
}

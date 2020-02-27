package nl.getthere.dataunit.iolink

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class PressureTest {
  @Test
  def testRound() : Unit = {
    assertEquals(13.37, Pressure.round(13.373847))
  }

  @Test
  def testPressure688(): Unit = {
    val data = "0AC0"
    val expected = 6.88
    assertEquals(expected, Pressure.extract(data))
  }

  @Test
  def testRounding(): Unit = {
    val data = "01C6"
    val expected = 1.13
    assertEquals(expected, Pressure.extract(data))
  }
}

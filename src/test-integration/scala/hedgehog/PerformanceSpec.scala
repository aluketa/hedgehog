package hedgehog

import java.util.concurrent.ConcurrentHashMap

import org.scalatest.{FunSpec, Matchers}

class PerformanceSpec extends FunSpec with Matchers {
  describe("Performance") {
    it("on average, should be no more than three times as slow as ConcurrentHashMap putting large values") {
      val concurrentHashMap = new ConcurrentHashMap[Integer, String]
      val hedgehogMap = HedgehogMap.createEphemeralMap[Integer, String]
      Timer.reset()

      (0 until 500).foreach(i => Timer.time(concurrentHashMap.put(i, "x" * 1024 * 1024)))
      val concurrentPutAverage = Timer.average
      Timer.reset()

      (0 until 500).foreach(i => Timer.time(hedgehogMap.put(i, "x" * 1024 * 1024)))
      val hedgehogPutAverage = Timer.average

      hedgehogPutAverage shouldBe < (concurrentPutAverage * 3.0)
    }

    it("on average, should take no more than 2ms to retrieve an item for a given index") {
      val hedgehogMap = HedgehogMap.createEphemeralMap[Integer, String]
      Timer.reset()
      (0 until 500).foreach(i => hedgehogMap.put(i, "x" * 1024 * 1024))

      (0 until 500).foreach(i => Timer.time(hedgehogMap.get(i)) shouldEqual "x" * 1024 * 1024)

      Timer.average shouldBe < (2.0)
    }

    it("Supports storage of data larger than Max Integer") {
      val hedgehogMap = HedgehogMap.createEphemeralMap[Integer, String]
      (0 until 2047 + 100).foreach(i => hedgehogMap.put(i, "x" * 1024 * 1024))
      hedgehogMap.size shouldBe 2047 + 100
    }
  }
}

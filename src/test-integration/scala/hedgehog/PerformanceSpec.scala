package hedgehog

import java.util.concurrent.ConcurrentHashMap

import org.scalatest.{FunSpec, Matchers}

class PerformanceSpec extends FunSpec with Matchers {
  describe("Performance") {
    it("on average, should be no more than two and a half times as slow as ConcurrentHashMap putting large values") {
      val concurrentHashMap = new ConcurrentHashMap[Integer, String]
      val hedgehogMap = HedgehogMap.createEphemeralMap[Integer, String]
      Timer.reset()

      (0 until 500).foreach(i => Timer.time(concurrentHashMap.put(1, "x" * 1024 * 1024)))
      val concurrentPutAverage = Timer.average
      Timer.reset()

      (0 until 500).foreach(i => Timer.time(hedgehogMap.put(1, "x" * 1024 * 1024)))
      val hedgehogPutAverage = Timer.average

      hedgehogPutAverage shouldBe < (concurrentPutAverage * 2.5)
    }
  }
}

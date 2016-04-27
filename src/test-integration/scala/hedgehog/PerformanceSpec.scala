package hedgehog

import org.scalatest.{FunSpec, Matchers}

class PerformanceSpec extends FunSpec with Matchers {
  describe("Performance") {
    it("it should be fast enough") {
      1 shouldEqual 1
    }
  }
}

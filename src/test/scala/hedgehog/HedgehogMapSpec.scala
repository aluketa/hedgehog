package hedgehog

import org.scalatest.{FunSpec, Matchers}

import scala.collection.JavaConversions._

class HedgehogMapSpec extends FunSpec with Matchers {
  describe("Hedgehog Map") {
    it("puts and gets a test value") {
      val map = new HedgehogMap[String, String]
      map.put("Test", "Data")

      map.get("Test") shouldEqual "Data"
    }

    it("updates an existing value") {
      val map = new HedgehogMap[String, String]
      map.put("Test", "Data") shouldBe null
      map.put("Test", "Updated Data") shouldEqual "Data"

      map.get("Test") shouldEqual "Updated Data"
    }

    it("indicates if the map contains a given key") {
      val map = new HedgehogMap[String, String]
      map.put("Test", "Data")

      map.containsKey("Test") shouldBe true
      map.containsKey("Unknown") shouldBe false
    }

    it("returns the key set") {
      val map = new HedgehogMap[String, String]
      map.put("Test1", "Data")
      map.put("Test2", "Data")
      map.put("Test3", "Data")

      asScalaSet(map.keySet()) shouldEqual Set("Test1", "Test2", "Test3")
    }

    it("contains a collection of values") {
      val map = new HedgehogMap[String, String]
      map.put("Test1", "Data1")
      map.put("Test2", "Data2")
      map.put("Test3", "Data3")

      collectionAsScalaIterable(map.values()) should contain theSameElementsAs Iterable("Data1", "Data2", "Data3")
    }

    it("returns the entry set") {
      val map = new HedgehogMap[String, String]
      map.put("Test1", "Data1")
      map.put("Test2", "Data2")
      map.put("Test3", "Data3")

      asScalaSet(map.entrySet()).map(e => (e.getKey, e.getValue)) shouldEqual Set(
        ("Test1", "Data1"),
        ("Test2", "Data2"),
        ("Test3", "Data3"))
    }

    it("returns the size of the map") {
      val map = new HedgehogMap[String, String]
      map.put("Test1", "Data1")
      map.put("Test2", "Data2")
      map.put("Test3", "Data3")

      map.size shouldBe 3
    }

    it("clears the map") {
      val map = new HedgehogMap[String, String]
      map.put("Test1", "Data1")
      map.put("Test2", "Data2")
      map.put("Test3", "Data3")

      map.clear()

      map.size shouldBe 0
      asScalaSet(map.entrySet()) shouldEqual Set()
    }

    it("removes an element from the map") {
      val map = new HedgehogMap[String, String]
      map.put("Test1", "Data1")
      map.put("Test2", "Data2")
      map.put("Test3", "Data3")

      map.remove("Test2") shouldEqual "Data2"
      asScalaSet(map.entrySet()).map(e => (e.getKey, e.getValue)) shouldEqual Set(
        ("Test1", "Data1"),
        ("Test3", "Data3"))
    }

    it("verifies that the map contains a given value") {
      val map = new HedgehogMap[String, String]
      map.put("Test1", "Data1")
      map.put("Test2", "Data2")
      map.put("Test3", "Data3")

      map.containsValue("Data2") shouldBe true
      map.containsValue("incorrect") shouldBe false
    }

    it("verifies if the map is empty") {
      val map = new HedgehogMap[String, String]
      map.isEmpty shouldBe true
      map.put("Test1", "Data1")
      map.isEmpty shouldBe false
    }

    it("puts all items into the map") {
      val map = new HedgehogMap[String, String]
      map.putAll(Map(
        "Test1" -> "Data1",
        "Test2" -> "Data2",
        "Test3" -> "Data3"))

      asScalaSet(map.entrySet()).map(e => (e.getKey, e.getValue)) shouldEqual Set(
        ("Test1", "Data1"),
        ("Test2", "Data2"),
        ("Test3", "Data3"))
    }

    it("stores data larger than the initial size of 1mb") {
      val map = new HedgehogMap[String, String]
      val value1 = "x" * 1024 * 1024
      val value2 = "y" * 1024 * 1024

      map.put("key1", value1)
      map.put("key2", value2)

      map.get("key1") shouldEqual value1
      map.get("key2") shouldEqual value2
    }
  }
}

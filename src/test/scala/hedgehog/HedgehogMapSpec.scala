package hedgehog

import java.nio.file.Files

import org.scalatest.{FunSpec, Matchers}

import scala.collection.JavaConversions._

class HedgehogMapSpec extends FunSpec with Matchers {
  describe("Hedgehog Map") {
    it("puts and gets a test value") {
      val map = HedgehogMap.createEphemeralMap[String, String]
      map.put("Test", "Data")

      map.get("Test") shouldEqual "Data"
    }

    it("updates an existing value") {
      val map = HedgehogMap.createEphemeralMap[String, String]
      map.put("Test", "Data") shouldBe null
      map.put("Test", "Updated Data") shouldEqual "Data"

      map.get("Test") shouldEqual "Updated Data"
    }

    it("indicates if the map contains a given key") {
      val map = HedgehogMap.createEphemeralMap[String, String]
      map.put("Test", "Data")

      map.containsKey("Test") shouldBe true
      map.containsKey("Unknown") shouldBe false
    }

    it("returns the key set") {
      val map = HedgehogMap.createEphemeralMap[String, String]
      map.put("Test1", "Data")
      map.put("Test2", "Data")
      map.put("Test3", "Data")

      asScalaSet(map.keySet()) shouldEqual Set("Test1", "Test2", "Test3")
    }

    it("contains a collection of values") {
      val map = HedgehogMap.createEphemeralMap[String, String]
      map.put("Test1", "Data1")
      map.put("Test2", "Data2")
      map.put("Test3", "Data3")

      collectionAsScalaIterable(map.values()) should contain theSameElementsAs Iterable("Data1", "Data2", "Data3")
    }

    it("returns the entry set") {
      val map = HedgehogMap.createEphemeralMap[String, String]
      map.put("Test1", "Data1")
      map.put("Test2", "Data2")
      map.put("Test3", "Data3")

      asScalaSet(map.entrySet()).map(e => (e.getKey, e.getValue)) shouldEqual Set(
        ("Test1", "Data1"),
        ("Test2", "Data2"),
        ("Test3", "Data3"))
    }

    it("returns the size of the map") {
      val map = HedgehogMap.createEphemeralMap[String, String]
      map.put("Test1", "Data1")
      map.put("Test2", "Data2")
      map.put("Test3", "Data3")

      map.size shouldBe 3
    }

    it("clears the map") {
      val map = HedgehogMap.createEphemeralMap[String, String]
      map.put("Test1", "Data1")
      map.put("Test2", "Data2")
      map.put("Test3", "Data3")

      map.clear()

      map.size shouldBe 0
      asScalaSet(map.entrySet()) shouldEqual Set()
    }

    it("removes an element from the map") {
      val map = HedgehogMap.createEphemeralMap[String, String]
      map.put("Test1", "Data1")
      map.put("Test2", "Data2")
      map.put("Test3", "Data3")

      map.remove("Test2") shouldEqual "Data2"
      asScalaSet(map.entrySet()).map(e => (e.getKey, e.getValue)) shouldEqual Set(
        ("Test1", "Data1"),
        ("Test3", "Data3"))
    }

    it("verifies that the map contains a given value") {
      val map = HedgehogMap.createEphemeralMap[String, String]
      map.put("Test1", "Data1")
      map.put("Test2", "Data2")
      map.put("Test3", "Data3")

      map.containsValue("Data2") shouldBe true
      map.containsValue("incorrect") shouldBe false
    }

    it("verifies if the map is empty") {
      val map = HedgehogMap.createEphemeralMap[String, String]
      map.isEmpty shouldBe true
      map.put("Test1", "Data1")
      map.isEmpty shouldBe false
    }

    it("puts all items into the map") {
      val map = HedgehogMap.createEphemeralMap[String, String]
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
      val map = HedgehogMap.createEphemeralMap[String, String]
      val value1 = "x" * 1024 * 1024
      val value2 = "y" * 1024 * 1024

      map.put("key1", value1)
      map.put("key2", value2)

      map.get("key1") shouldEqual value1
      map.get("key2") shouldEqual value2
    }

    it("creates a persistent map") {
      val tempDir = Files.createTempDirectory("hdg")
      try {
        val map1 = HedgehogMap.createPersistentMap[String, String](tempDir, "test-map")
        map1.put("Test Key", "Test Value")

        val map2 = HedgehogMap.createPersistentMap[String, String](tempDir, "test-map")
        map2.get("Test Key") shouldEqual "Test Value"
      } finally {
        Files.delete(tempDir.resolve("map-test-map.hdg"))
        Files.delete(tempDir.resolve("idx-test-map.hdg"))
        Files.delete(tempDir)
      }
    }

    it("successfully appends to a restored persistent map") {
      val tempDir = Files.createTempDirectory("hdg")
      try {
        val map1 = HedgehogMap.createPersistentMap[String, String](tempDir, "test-map")
        map1.put("Test Key", "Test Value")

        val map2 = HedgehogMap.createPersistentMap[String, String](tempDir, "test-map")
        map2.put("Test Key 2", "Test Value 2")

        map2.entrySet.map(e => (e.getKey, e.getValue)) shouldEqual Set(
          ("Test Key", "Test Value"),
          ("Test Key 2", "Test Value 2"))
      } finally {
        Files.delete(tempDir.resolve("map-test-map.hdg"))
        Files.delete(tempDir.resolve("idx-test-map.hdg"))
        Files.delete(tempDir)
      }
    }

    it("supports restoration of persistent maps that have been re-sized") {
      val tempDir = Files.createTempDirectory("hdg")
      try {
        val value1 = "x" * 1024 * 1024
        val value2 = "y" * 1024 * 1024

        val map1 = HedgehogMap.createPersistentMap[String, String](tempDir, "test-map")
        map1.put("key1", value1)
        map1.put("key2", value2)

        val map2 = HedgehogMap.createPersistentMap[String, String](tempDir, "test-map")
        map2.get("key1") shouldEqual value1
        map2.get("key2") shouldEqual value2
      } finally {
        Files.delete(tempDir.resolve("map-test-map.hdg"))
        Files.delete(tempDir.resolve("idx-test-map.hdg"))
        Files.delete(tempDir)
      }
    }

    it("restores an empty persistent map") {
      val tempDir = Files.createTempDirectory("hdg")
      try {
        val map1 = HedgehogMap.createPersistentMap[String, String](tempDir, "test-map")
        map1.size shouldBe 0

        val map2 = HedgehogMap.createPersistentMap[String, String](tempDir, "test-map")
        map2.size shouldBe 0
        map2.put("key1", "value1")
        map2.get("key1") shouldEqual "value1"
      } finally {
        Files.delete(tempDir.resolve("map-test-map.hdg"))
        Files.delete(tempDir.resolve("idx-test-map.hdg"))
        Files.delete(tempDir)
      }
    }

    it("sets the correct size when restoring an existing persistent map") {
      val tempDir = Files.createTempDirectory("hdg")
      try {
        val map1 = HedgehogMap.createPersistentMap[String, String](tempDir, "test-map")
        map1.put("key1", "value1")
        map1.put("key2", "value2")
        map1.put("key3", "value3")

        val map2 = HedgehogMap.createPersistentMap[String, String](tempDir, "test-map")
        map2.size shouldBe 3
      } finally {
        Files.delete(tempDir.resolve("map-test-map.hdg"))
        Files.delete(tempDir.resolve("idx-test-map.hdg"))
        Files.delete(tempDir)
      }
    }
  }
}

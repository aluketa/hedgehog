package hedgehog

import java.nio.file.Files

import org.scalatest.{FunSpec, Matchers}

import scala.collection.JavaConversions._

class HedgehogMapSpec extends FunSpec with Matchers {
  describe("Hedgehog Map") {
    it("puts and gets a test value") {
      val map = HedgehogMap.createEphemeralMap[String, String]()
      map.put("Test", "Data")

      map.get("Test") shouldEqual "Data"
    }

    it("updates an existing value") {
      val map = HedgehogMap.createEphemeralMap[String, String]()
      map.put("Test", "Data") shouldBe null
      map.put("Test", "Updated Data") shouldEqual "Data"

      map.get("Test") shouldEqual "Updated Data"
    }

    it("indicates if the map contains a given key") {
      val map = HedgehogMap.createEphemeralMap[String, String]()
      map.put("Test", "Data")

      map.containsKey("Test") shouldBe true
      map.containsKey("Unknown") shouldBe false
    }

    it("returns the key set") {
      val map = HedgehogMap.createEphemeralMap[String, String]()
      map.put("Test1", "Data")
      map.put("Test2", "Data")
      map.put("Test3", "Data")

      asScalaSet(map.keySet()) shouldEqual Set("Test1", "Test2", "Test3")
    }

    it("contains a collection of values") {
      val map = HedgehogMap.createEphemeralMap[String, String]()
      map.put("Test1", "Data1")
      map.put("Test2", "Data2")
      map.put("Test3", "Data3")

      collectionAsScalaIterable(map.values()) should contain theSameElementsAs Iterable("Data1", "Data2", "Data3")
    }

    it("returns the entry set") {
      val map = HedgehogMap.createEphemeralMap[String, String]()
      map.put("Test1", "Data1")
      map.put("Test2", "Data2")
      map.put("Test3", "Data3")

      asScalaSet(map.entrySet()).map(e => (e.getKey, e.getValue)) shouldEqual Set(
        ("Test1", "Data1"),
        ("Test2", "Data2"),
        ("Test3", "Data3"))
    }

    it("returns the size of the map") {
      val map = HedgehogMap.createEphemeralMap[String, String]()
      map.put("Test1", "Data1")
      map.put("Test2", "Data2")
      map.put("Test3", "Data3")

      map.size shouldBe 3
    }

    it("clears the map") {
      val map = HedgehogMap.createEphemeralMap[String, String]()
      map.put("Test1", "Data1")
      map.put("Test2", "Data2")
      map.put("Test3", "Data3")

      map.clear()

      map.size shouldBe 0
      asScalaSet(map.entrySet()) shouldEqual Set()
    }

    it("removes an element from the map") {
      val map = HedgehogMap.createEphemeralMap[String, String]()
      map.put("Test1", "Data1")
      map.put("Test2", "Data2")
      map.put("Test3", "Data3")

      map.remove("Test2") shouldEqual "Data2"
      asScalaSet(map.entrySet()).map(e => (e.getKey, e.getValue)) shouldEqual Set(
        ("Test1", "Data1"),
        ("Test3", "Data3"))
    }

    it("verifies that the map contains a given value") {
      val map = HedgehogMap.createEphemeralMap[String, String]()
      map.put("Test1", "Data1")
      map.put("Test2", "Data2")
      map.put("Test3", "Data3")

      map.containsValue("Data2") shouldBe true
      map.containsValue("incorrect") shouldBe false
    }

    it("verifies if the map is empty") {
      val map = HedgehogMap.createEphemeralMap[String, String]()
      map.isEmpty shouldBe true
      map.put("Test1", "Data1")
      map.isEmpty shouldBe false
    }

    it("puts all items into the map") {
      val map = HedgehogMap.createEphemeralMap[String, String]()
      map.putAll(Map(
        "Test1" -> "Data1",
        "Test2" -> "Data2",
        "Test3" -> "Data3"))

      asScalaSet(map.entrySet()).map(e => (e.getKey, e.getValue)) shouldEqual Set(
        ("Test1", "Data1"),
        ("Test2", "Data2"),
        ("Test3", "Data3"))
    }

    it("stores data larger than the initial size of 1mb (ephemeral)") {
      val map = HedgehogMap.createEphemeralMap[String, String]()
      val value1 = "x" * 1024 * 1024
      val value2 = "y" * 1024 * 1024

      map.put("key1", value1)
      map.put("key2", value2)

      map.get("key1") shouldEqual value1
      map.get("key2") shouldEqual value2
    }

    it("stores data larger than the initial size of 1mb (persistent)") {
      val tempDir = Files.createTempDirectory("hdg")
      try {
        val map = HedgehogMap.createPersistentMap[String, String](tempDir, "test-map")
        val value1 = "x" * 1024 * 1024
        val value2 = "y" * 1024 * 1024

        map.put("key1", value1)
        map.put("key2", value2)

        map.get("key1") shouldEqual value1
        map.get("key2") shouldEqual value2
      } finally {
        Files.delete(tempDir.resolve("map-test-map.hdg"))
        Files.delete(tempDir.resolve("idx-test-map.hdg"))
        Files.delete(tempDir)
      }
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

        //println(map1.keySet)

        val map2 = HedgehogMap.createPersistentMap[String, String](tempDir, "test-map")
        //println(map2.get("key1"))
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

    it("compacts the map") {
      val tempDir = Files.createTempDirectory("hdg")
      try {
        val map = HedgehogMap.createPersistentMap[String, String](tempDir, "test-map")
        map.put("x" * 1024 * 1024, "a" * 1024 * 1024)
        map.put("y" * 1024 * 1024, "b" * 1024 * 1024)
        map.put("z" * 1024 * 1024, "c" * 1024 * 1024)
        Files.size(tempDir.resolve("map-test-map.hdg")) shouldBe 4194304
        Files.size(tempDir.resolve("idx-test-map.hdg")) shouldBe 9437184

        map.compact()
        map.force()
        map.size shouldBe 3
        Files.size(tempDir.resolve("map-test-map.hdg")) shouldBe 3145728
        Files.size(tempDir.resolve("idx-test-map.hdg")) shouldBe 3158503
        map.get("x" * 1024 * 1024) shouldEqual "a" * 1024 * 1024
        map.get("y" * 1024 * 1024) shouldEqual "b" * 1024 * 1024
        map.get("z" * 1024 * 1024) shouldEqual "c" * 1024 * 1024

        map.remove("x" * 1024 * 1024)
        map.compact()
        map.force()
        map.size shouldBe 2
        map.get("y" * 1024 * 1024) shouldEqual "b" * 1024 * 1024
        map.get("z" * 1024 * 1024) shouldEqual "c" * 1024 * 1024
        Files.size(tempDir.resolve("map-test-map.hdg")) shouldBe 2097152
        Files.size(tempDir.resolve("idx-test-map.hdg")) shouldBe 3170823

        map.remove("y" * 1024 * 1024)
        map.compact()
        map.force()
        map.size shouldBe 1
        map.get("z" * 1024 * 1024) shouldEqual "c" * 1024 * 1024
        Files.size(tempDir.resolve("map-test-map.hdg")) shouldBe 1048576
        Files.size(tempDir.resolve("idx-test-map.hdg")) shouldBe 3145728

        map.remove("z" * 1024 * 1024)
        map.compact()
        map.force()
        map.size shouldBe 0
        Files.size(tempDir.resolve("map-test-map.hdg")) shouldBe 1048576
        Files.size(tempDir.resolve("idx-test-map.hdg")) shouldBe 1048576
      } finally {
        Files.delete(tempDir.resolve("map-test-map.hdg"))
        Files.delete(tempDir.resolve("idx-test-map.hdg"))
        Files.delete(tempDir)
      }
    }

    it("compacts an empty map") {
      val tempDir = Files.createTempDirectory("hdg")
      try {
        val map = HedgehogMap.createPersistentMap[String, String](tempDir, "test-map")

        map.compact()
        map.force()
        map.size shouldBe 0
        Files.size(tempDir.resolve("map-test-map.hdg")) shouldBe 1024 * 1024
        Files.size(tempDir.resolve("idx-test-map.hdg")) shouldBe 1024 * 1024
      } finally {
        Files.delete(tempDir.resolve("map-test-map.hdg"))
        Files.delete(tempDir.resolve("idx-test-map.hdg"))
        Files.delete(tempDir)
      }
    }

    it("compacts an ephemeral map") {
      val map = HedgehogMap.createEphemeralMap[String, String]()
      map.compact()
      map.force()
      map.size shouldBe 0

      map.put("Key", "Value")
      map.get("Key") shouldEqual "Value"
    }

    it("accepts new additions following compaction") {
      val tempDir = Files.createTempDirectory("hdg")
      try {
        val map = HedgehogMap.createPersistentMap[String, String](tempDir, "test-map")

        map.compact()
        map.force()
        map.size shouldBe 0
        Files.size(tempDir.resolve("map-test-map.hdg")) shouldBe 1024 * 1024
        Files.size(tempDir.resolve("idx-test-map.hdg")) shouldBe 1024 * 1024

        map.put("key", "value")
        map.size shouldBe 1
        map.get("key") shouldEqual "value"
      } finally {
        Files.delete(tempDir.resolve("map-test-map.hdg"))
        Files.delete(tempDir.resolve("idx-test-map.hdg"))
        Files.delete(tempDir)
      }
    }

    it("replaces keys with a given value if the current value equals a given prior value") {
      val map = HedgehogMap.createEphemeralMap[String, String]()
      map.put("test", "old-value")
      map.replace("test", "old-value", "new-value")

      map.get("test") shouldEqual "new-value"
    }

    it("does not replace keys if the given prior value does not match the current value") {
      val map = HedgehogMap.createEphemeralMap[String, String]()
      map.put("test", "old-value")
      map.replace("test", "incorrect-value", "new-value")

      map.get("test") shouldEqual "old-value"
    }

    it("replace a value regardless of the current value") {
      val map = HedgehogMap.createEphemeralMap[String, String]()
      map.put("test", "old-value")
      map.replace("test", "new-value")

      map.get("test") shouldEqual "new-value"
    }

    it("removes a key if the corresponding current value matches the given value") {
      val map = HedgehogMap.createEphemeralMap[String, String]()
      map.put("test", "current-value")

      withClue("map remove returned")(map.remove("test", "current-value") shouldBe true)
      withClue("contains test key")(map.containsKey("test") shouldBe false)
    }

    it("does not remove a key if the corresponding current value does not match the given value") {
      val map = HedgehogMap.createEphemeralMap[String, String]()
      map.put("test", "current-value")

      withClue("map remove returned")(map.remove("test", "incorrect-value") shouldBe false)
      withClue("contains test key")(map.containsKey("test") shouldBe true)
    }

    it("puts a given value if the key is current absent") {
      val map = HedgehogMap.createEphemeralMap[String, String]()
      map.putIfAbsent("test", "new value") shouldBe null
      map.get("test") shouldEqual "new value"
    }

    it("does not put a given value if the key already exists") {
      val map = HedgehogMap.createEphemeralMap[String, String]()
      map.put("test", "current value")
      map.putIfAbsent("test", "new value") shouldEqual "current value"
      map.get("test") shouldEqual "current value"
    }
  }
}

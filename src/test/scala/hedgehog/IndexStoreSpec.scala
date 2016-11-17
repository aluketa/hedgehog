package hedgehog

import java.nio.file.Files

import org.scalatest.{FunSpec, Matchers}

class IndexStoreSpec extends FunSpec with Matchers {
  describe("Index Store") {
    it("puts and gets a key") {
      val indexStore = new IndexStore[String]
      indexStore.put("testKey1", 42, 3)
      indexStore.get("testKey1") shouldBe Some((42, 3))
    }

    it("puts and gets multiple interleaved keys") {
      val indexStore = new IndexStore[String]
      indexStore.put("testKey1", 42, 3)
      indexStore.put("testKey456", 43, 5)
      indexStore.get("testKey1") shouldBe Some((42, 3))
      indexStore.get("testKey456") shouldBe Some((43, 5))
      indexStore.put("testKey2", 44, 7)
      indexStore.get("testKey2") shouldBe Some((44, 7))
      indexStore.put("testKey3", 45, 11)
      indexStore.get("testKey3") shouldBe Some((45, 11))
    }

    it("returns None if a given key does not exist") {
      val indexStore = new IndexStore[String]
      indexStore.get("non-existent") shouldBe None
    }

    it("overwrites an existing key") {
      val indexStore = new IndexStore[String]
      indexStore.put("testKey1", 42, 3)
      indexStore.put("testKey1", 43, 5)

      indexStore.get("testKey1") shouldBe Some((43, 5))
      indexStore.size shouldBe 1
      indexStore.entries shouldEqual Seq(("testKey1", (43, 5)))
    }

    it("correctly stores and retrieves different keys with identical hash codes") {
      val indexStore = new IndexStore[IdenticalHashCodeKey]

      indexStore.put(IdenticalHashCodeKey("testKey1"), 42, 3)
      indexStore.put(IdenticalHashCodeKey("testKey2"), 43, 5)
      indexStore.put(IdenticalHashCodeKey("testKey3"), 44, 7)

      indexStore.get(IdenticalHashCodeKey("testKey1")) shouldBe Some((42, 3))
      indexStore.get(IdenticalHashCodeKey("testKey2")) shouldBe Some((43, 5))
      indexStore.get(IdenticalHashCodeKey("testKey3")) shouldBe Some((44, 7))
    }

    it("returns the correct size") {
      val indexStore = new IndexStore[String]
      indexStore.put("testKey1", 1, 1)
      indexStore.put("testKey2", 2, 2)
      indexStore.put("testKey3", 3, 3)

      indexStore.size shouldBe 3
    }

    it("returns all entries") {
      val indexStore = new IndexStore[String]
      indexStore.put("testKey1", 1, 1)
      indexStore.put("testKey2", 2, 2)
      indexStore.put("testKey3", 3, 3)

      indexStore.entries.toSet shouldEqual Set(
        ("testKey1", (1, 1)),
        ("testKey2", (2, 2)),
        ("testKey3", (3, 3)))
    }

    it("indicated if a store contains a given key") {
      val indexStore = new IndexStore[String]
      indexStore.put("testKey1", 1, 1)

      indexStore.contains("testKey1") shouldBe true
      indexStore.contains("non-existent") shouldBe false
    }

    it("clears the index store") {
      val indexStore = new IndexStore[String]
      indexStore.put("testKey1", 1, 1)
      indexStore.put("testKey2", 2, 2)
      indexStore.put("testKey3", 3, 3)
      indexStore.clear()

      indexStore.size shouldBe 0
      indexStore.entries shouldBe empty
      indexStore.contains("testKey1") shouldBe false
      indexStore.contains("testKey2") shouldBe false
      indexStore.contains("testKey3") shouldBe false
    }

    it("removes an item from the store") {
      val indexStore = new IndexStore[String]
      indexStore.put("testKey1", 1, 1)
      indexStore.put("testKey2", 2, 2)

      indexStore.remove("testKey1")

      indexStore.size shouldBe 1
      indexStore.entries shouldEqual Seq(("testKey2", (2, 2)))
      indexStore.contains("testKey1") shouldBe false
      indexStore.contains("testKey2") shouldBe true
    }

    it("removing a non-existing item does not impact store size") {
      val indexStore = new IndexStore[String]
      indexStore.put("testKey1", 1, 1)

      indexStore.remove("non-existent")
      indexStore.size shouldBe 1
    }

    it("supports entry of more keys than the initial capacity of 1024 entries") {
      val indexStore = new IndexStore[String]
      (0 until 2048).foreach(i => indexStore.put(s"key$i", i * 31, i))
      (0 until 2048).foreach(i => indexStore.get(s"key$i") shouldBe Some((i * 31, i)))
    }

    it("supports entry of keys more than the initial size of 1mb") {
      val key1: String = "x" * 1024 * 1024
      val key2: String = "y" * 1024 * 1024

      val indexStore = new IndexStore[String]
      indexStore.put(key1, 1, 1)
      indexStore.put(key2, 2, 2)

      indexStore.get(key1) shouldBe Some((1, 1))
      indexStore.get(key2) shouldBe Some((2, 2))
    }

    it("stores an item with a hashcode of zero (ensures we preserve capacity at position 0)") {
      val indexStore = new IndexStore[Integer]
      indexStore.put(0, 42, 7)
      indexStore.get(0) shouldBe Some((42, 7))
    }

    it("creates a persistent index store") {
      val filename = Files.createTempFile("idx-", ".hdg")
      try {
        val indexStore1 = new IndexStore[String](filename = filename, deleteOnClose = false)
        indexStore1.put("Test Key", 1, 1)

        val indexStore2 = new IndexStore[String](filename = filename, deleteOnClose = false)
        indexStore2.get("Test Key") shouldBe Some((1, 1))
      } finally {
        Files.delete(filename)
      }
    }

    it("successfully appends to the end of a restored persistent index store") {
      val filename = Files.createTempFile("idx-", ".hdg")
      try {
        val indexStore1 = new IndexStore[String](filename = filename, deleteOnClose = false)
        indexStore1.put("Test Key", 1, 1)

        val indexStore2 = new IndexStore[String](filename = filename, deleteOnClose = false)
        indexStore2.put("Test Key2", 2, 2)

        indexStore2.entries.toSet shouldEqual Set(
          ("Test Key", (1, 1)),
          ("Test Key2", (2, 2)))
      } finally {
        Files.delete(filename)
      }
    }

    it("supports restoration of persistent stores that have been re-sized") {
      val filename = Files.createTempFile("idx-", ".hdg")
      try {
        val key1: String = "x" * 1024 * 1024
        val key2: String = "y" * 1024 * 1024

        val indexStore1 = new IndexStore[String](filename = filename, deleteOnClose = false)
        indexStore1.put(key1, 1, 1)
        indexStore1.put(key2, 2, 2)

        val indexStore2 = new IndexStore[String](filename = filename, deleteOnClose = false)
        indexStore2.get(key1) shouldBe Some((1, 1))
        indexStore2.get(key2) shouldBe Some((2, 2))
      } finally {
        Files.delete(filename)
      }
    }

    it("restores an empty persistent store (ensures restoration correctly handles max pos of zero)") {
      val filename = Files.createTempFile("idx-", ".hdg")
      try {
        val indexStore1 = new IndexStore[String](filename = filename, deleteOnClose = false)
        indexStore1.size shouldBe 0

        val indexStore2 = new IndexStore[String](filename = filename, deleteOnClose = false)
        indexStore2.size shouldBe 0
        indexStore2.put("Test", 1, 1)
        indexStore2.get("Test") shouldBe Some(1, 1)
      } finally {
        Files.delete(filename)
      }
    }

    it("sets the correct index size when restoring an existing persistent store") {
      val filename = Files.createTempFile("idx-", ".hdg")
      try {
        val indexStore1 = new IndexStore[String](filename = filename, deleteOnClose = false)
        indexStore1.put("Test1", 1, 1)
        indexStore1.put("Test2", 2, 2)
        indexStore1.put("Test3", 3, 3)

        val indexStore2 = new IndexStore[String](filename = filename, deleteOnClose = false)
        indexStore2.size shouldBe 3
      } finally {
        Files.delete(filename)
      }
    }
  }
}

case class IdenticalHashCodeKey(value: String) {
  override def hashCode(): Int = 42
}

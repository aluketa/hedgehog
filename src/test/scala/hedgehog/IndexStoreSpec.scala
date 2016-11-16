package hedgehog

import org.scalatest.{FunSpec, Matchers}

class IndexStoreSpec extends FunSpec with Matchers {
  describe("Index Store") {
    it("puts and gets a key") {
      val indexStore = new IndexStore[String]
      indexStore.put("testKey1", 42L, 3)
      indexStore.get("testKey1") shouldBe Some((42L, 3))
    }

    it("puts and gets multiple interleaved keys") {
      val indexStore = new IndexStore[String]
      indexStore.put("testKey1", 42L, 3)
      indexStore.put("testKey456", 43L, 5)
      indexStore.get("testKey1") shouldBe Some((42L, 3))
      indexStore.get("testKey456") shouldBe Some((43L, 5))
      indexStore.put("testKey2", 44L, 7)
      indexStore.get("testKey2") shouldBe Some((44L, 7))
      indexStore.put("testKey3", 45L, 11)
      indexStore.get("testKey3") shouldBe Some((45L, 11))
    }

    it("returns None if a given key does not exist") {
      val indexStore = new IndexStore[String]
      indexStore.get("non-existent") shouldBe None
    }

    it("overwrites an existing key") {
      val indexStore = new IndexStore[String]
      indexStore.put("testKey1", 42L, 3)
      indexStore.put("testKey1", 43L, 5)

      indexStore.get("testKey1") shouldBe Some((43L, 5))
      indexStore.size shouldBe 1
      indexStore.entries shouldEqual Seq(("testKey1", (43L, 5)))
    }

    it("correctly stores and retrieves different keys with identical hash codes") {
      val indexStore = new IndexStore[IdenticalHashCodeKey]

      indexStore.put(IdenticalHashCodeKey("testKey1"), 42L, 3)
      indexStore.put(IdenticalHashCodeKey("testKey2"), 43L, 5)
      indexStore.put(IdenticalHashCodeKey("testKey3"), 44L, 7)

      indexStore.get(IdenticalHashCodeKey("testKey1")) shouldBe Some((42L, 3))
      indexStore.get(IdenticalHashCodeKey("testKey2")) shouldBe Some((43L, 5))
      indexStore.get(IdenticalHashCodeKey("testKey3")) shouldBe Some((44L, 7))
    }

    it("returns the correct size") {
      val indexStore = new IndexStore[String]
      indexStore.put("testKey1", 1L, 1)
      indexStore.put("testKey2", 2L, 2)
      indexStore.put("testKey3", 3L, 3)

      indexStore.size shouldBe 3
    }

    it("returns all entries") {
      val indexStore = new IndexStore[String]
      indexStore.put("testKey1", 1L, 1)
      indexStore.put("testKey2", 2L, 2)
      indexStore.put("testKey3", 3L, 3)

      indexStore.entries.toSet shouldEqual Set(
        ("testKey1", (1L, 1)),
        ("testKey2", (2L, 2)),
        ("testKey3", (3L, 3)))
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
      indexStore.put(key1, 1L, 1)
      indexStore.put(key2, 2L, 2)

      indexStore.get(key1) shouldBe Some((1L, 1))
      indexStore.get(key2) shouldBe Some((2L, 2))
    }
  }
}

case class IdenticalHashCodeKey(value: String) {
  override def hashCode(): Int = 42
}

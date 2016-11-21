package hedgehog

import java.nio.file.Files

import org.scalatest.{FunSpec, Matchers}

class LargeMappedByteBufferSpec extends FunSpec with Matchers {
  describe("Large Mapped Byte Buffer") {
    it("gets and retrieves an item within a single shard") {
      val data = "Test Data"
      val buffer = new LargeMappedByteBuffer(Files.createTempFile("map-", ".hdg"), 1024, isPersistent = false, maxShardSize = Int.MaxValue)
      buffer.put(data.getBytes)
      buffer.position shouldBe data.length

      val in = new Array[Byte](data.length)
      buffer.position(0)
      buffer.get(in)
      new String(in) shouldEqual data
    }

    it("gets and retrieves multiple items within a single shard") {
      val data = "Test Data"
      val buffer = new LargeMappedByteBuffer(Files.createTempFile("map-", ".hdg"), 1024, isPersistent = false, maxShardSize = Int.MaxValue)
      (1 to 3).foreach { i =>
        buffer.put(data.getBytes)
        buffer.position shouldBe data.length * i
      }

      buffer.position(0)
      (1 to 3).foreach { i =>
        val in = new Array[Byte](data.length)
        buffer.get(in)
        new String(in) shouldEqual data
        buffer.position shouldBe data.length * i
      }
    }

    it("gets and retrieves three items each occupying one of three shards") {
      val data = "x" * 1024
      val buffer = new LargeMappedByteBuffer(Files.createTempFile("map-", ".hdg"), 1024 * 3, isPersistent = false, maxShardSize = 1024)
      (1 to 3).foreach { i =>
        buffer.put(data.getBytes)
        buffer.position shouldBe data.length * i

      }

      buffer.position(0)
      (1 to 3).foreach { i =>
        val in = new Array[Byte](data.length)
        buffer.get(in)
        new String(in) shouldEqual data
        buffer.position shouldBe data.length * i
      }
    }

    it("gets and retrieves two items spanning three shards") {
      val data = "x" * (1024 + (1024 / 2))
      val buffer = new LargeMappedByteBuffer(Files.createTempFile("map-", ".hdg"), 1024 * 3, isPersistent = false, maxShardSize = 1024)
      (1 to 3).foreach { i =>
        buffer.put(data.getBytes)
        buffer.position shouldBe data.length * i
      }

      buffer.position(0)
      (1 to 3).foreach { i =>
        val in = new Array[Byte](data.length)
        buffer.get(in)
        new String(in) shouldEqual data
        buffer.position shouldBe data.length * i
      }
    }

    it("ensures the final shard consists of the correct size") {
      val buffer = new LargeMappedByteBuffer(Files.createTempFile("map-", ".hdg"), (5 * 1024 * 1024) + 42, isPersistent = false, maxShardSize = 1024)
      buffer.capacity shouldBe ((5 * 1024 * 1024) + 42)
    }

    it("returns the correct capacity for a single shard") {
      val buffer = new LargeMappedByteBuffer(Files.createTempFile("map-", ".hdg"), 5 * 1024 * 1024, isPersistent = false, maxShardSize = Int.MaxValue)
      buffer.capacity shouldBe 5 * 1024 * 1024
    }

    it("returns the correct capacity for multiple shards") {
      val buffer = new LargeMappedByteBuffer(Files.createTempFile("map-", ".hdg"), 5 * 1024 * 1024, isPersistent = false, maxShardSize = 1024)
      buffer.capacity shouldBe 5 * 1024 * 1024
    }

    it("sets and returns the correct position for a single shard") {
      val data = "Test Data"
      val buffer = new LargeMappedByteBuffer(Files.createTempFile("map-", ".hdg"), 1024, isPersistent = false, maxShardSize = Int.MaxValue)
      buffer.position(512)
      buffer.put(data.getBytes)
      buffer.position shouldBe (512 + data.length)

      buffer.position(512)
      val in = new Array[Byte](data.length)
      buffer.get(in)
      new String(in) shouldEqual data
    }

    it("sets and returns the correct position for multiple shards") {
      val data = "Test Data"
      val buffer = new LargeMappedByteBuffer(Files.createTempFile("map-", ".hdg"), 1024, isPersistent = false, maxShardSize = 128)
      buffer.position(512)
      buffer.put(data.getBytes)
      buffer.position shouldBe (512 + data.length)

      buffer.position(512)
      val in = new Array[Byte](data.length)
      buffer.get(in)
      new String(in) shouldEqual data
    }
  }
}

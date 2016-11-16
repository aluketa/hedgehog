package hedgehog

import java.io.{Serializable => JavaSerializable}
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode.READ_WRITE
import java.nio.file.StandardOpenOption._
import java.nio.file.{Files, Path}

import scala.annotation.tailrec
import scala.math.max

class IndexStore[K <: JavaSerializable](
    filename: Path = Files.createTempFile("idx-", ".hdg"),
    initialCapacity: Int = 0,
    initialFileSizeBytes: Long = 0) {
  private var capacity = max(initialCapacity, 1024)
  private var currentSize = 0

  private var buffer: MappedByteBuffer = {
    val fc = FileChannel.open(filename, CREATE, READ, WRITE, DELETE_ON_CLOSE)
    val fileSizeBytes = max(initialFileSizeBytes, 1024 * 1024)
    try { fc.map(READ_WRITE, 0, fileSizeBytes) } finally { fc.close() }
  }
  clear()

  def get(key: K): Option[(Int, Int)] =
    findIndex(key).map { case (_, ih) => (ih.valuePosition, ih.valueLength) }

  def put(key: K, valuePosition: Int, valueLength: Int): Unit = {
    val indexHolderBytes: Array[Byte] = IndexHolder(key, valuePosition, valueLength).getBytes

    if (currentSize > capacity / 2) {
      grow(capacity + (capacity << 1), buffer.capacity)
    }

    if (buffer.capacity < (buffer.position + indexHolderBytes.length + 4)) {
      grow(capacity, max(buffer.capacity + indexHolderBytes.length, buffer.capacity + (buffer.capacity << 1)))
    }

    val writePosition = buffer.position
    buffer.putInt(indexHolderBytes.length)
    buffer.put(indexHolderBytes)

    val (index, isNewEntry) = nextIndex(key)
    putIntByIndex(index, writePosition)
    if (isNewEntry) {
      currentSize = currentSize + 1
    }
    buffer.force()
  }

  def entries: Iterable[(K, (Int, Int))] =
    (0 until capacity)
      .map(i => getIntByIndex(i))
      .filter(_ != 0)
      .map(p => indexHolderFor(p))
      .map(h => (h.key, (h.valuePosition, h.valueLength)))

  def size: Int = currentSize

  def contains(key: K): Boolean = findIndex(key).isDefined

  def clear(): Unit = {
    capacity = max(initialCapacity, 1024)
    currentSize = 0
    buffer.position(0)
    (0 until capacity).foreach(_ => buffer.putInt(0))
  }

  def remove(key: K): Unit = {
    findIndex(key) match {
      case Some((i, _)) =>
        putIntByIndex(i, 0)
        currentSize = currentSize - 1
      case _ => Unit
    }
  }

  @tailrec
  private def nextIndex(key: K, offset: Int = 0): (Int, Boolean) = {
    if (offset > capacity) {
      throw new IllegalStateException("Unable to locate a free index entry")
    }

    val index = ((key.hashCode.abs % capacity) + offset) % capacity
    val indexValue = getIntByIndex(index)
    if(indexValue == 0L) {
      (index, true)
    } else if (indexHolderFor(indexValue).key == key) {
      (index, false)
    } else {
      nextIndex(key, offset + 1)
    }
  }

  @tailrec
  private def findIndex(key: K, offset: Int = 0): Option[(Int, IndexHolder[K])] = {
    val index = ((key.hashCode.abs % capacity) + offset) % capacity
    val indexValue = getIntByIndex(index)
    if (indexValue == 0L) {
      None
    } else {
      val indexHolder = indexHolderFor(indexValue)
      if (indexHolder.key == key) {
        Some((index, indexHolder))
      } else {
        findIndex(key, offset + 1)
      }
    }
  }

  private def indexHolderFor(position: Int): IndexHolder[K] = {
    val mark = buffer.position
    buffer.position(position)

    val length = buffer.getInt()
    val data = new Array[Byte](length)
    buffer.get(data)
    buffer.position(mark)
    IndexHolder[K](data)
  }

  private def getIntByIndex(index: Int): Int = {
    val mark = buffer.position
    buffer.position(index * 4)
    try { buffer.getInt } finally { buffer.position(mark) }
  }

  private def putIntByIndex(index: Int, n: Int): Unit = {
    val mark = buffer.position
    buffer.position(index * 4)
    buffer.putInt(n)
    buffer.position(mark)
  }

  private def grow(newCapacity: Int, newFileSize: Long): Unit = {
    val tempStore = new IndexStore[K](Files.createTempFile("idx-", ".hdg"), newCapacity, newFileSize)
    entries.foreach { case (k, (p, l)) => tempStore.put(k, p, l) }
    val newStore = new IndexStore[K](filename, newCapacity, newFileSize)
    tempStore.entries.foreach { case (k, (p, l)) => newStore.put(k, p, l) }

    capacity = newCapacity
    buffer = newStore.buffer
    buffer.force()
  }
}

object IndexHolder {
  def apply[K <: JavaSerializable](bytes: Array[Byte]): IndexHolder[K] = bytesToValue(bytes)
}

case class IndexHolder[K <: JavaSerializable](key: K, valuePosition: Int, valueLength: Int) {
  def getBytes: Array[Byte] = valueToBytes(this)
}
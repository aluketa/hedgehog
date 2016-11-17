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
    initialFileSizeBytes: Long = 0,
    deleteOnClose: Boolean = true) {
  private var currentCapacity: Int = max(initialCapacity, 1024)
  private var currentSize: Int = 0

  private var currentBuffer: MappedByteBuffer = {
    val openOptions = Seq(CREATE, READ, WRITE) ++ (if (deleteOnClose) Seq(DELETE_ON_CLOSE) else Seq())
    val fc = FileChannel.open(filename, openOptions:_*)
    val fileSizeBytes: Long = Seq(initialFileSizeBytes, 1024L * 1024L, fc.size).max
    try { fc.map(READ_WRITE, 0, fileSizeBytes) } finally { fc.close() }
  }

  if (currentBuffer.getInt == 0) {
    clear()
  } else {
    restore()
  }

  def get(key: K): Option[(Int, Int)] =
    findIndex(key).map { case (_, ih) => (ih.valuePosition, ih.valueLength) }

  def put(key: K, valuePosition: Int, valueLength: Int): Unit = {
    val indexHolder = IndexHolder(key, valuePosition, valueLength)

    if (currentSize > currentCapacity / 2) {
      grow(currentCapacity + (currentCapacity << 1), currentBuffer.capacity)
    }

    if (currentBuffer.capacity < (currentBuffer.position + indexHolder.bytes.length + 4)) {
      grow(currentCapacity, max(currentBuffer.capacity + indexHolder.bytes.length, currentBuffer.capacity + (currentBuffer.capacity << 1)))
    }

    if (put(indexHolder, currentBuffer, currentCapacity)) {
      currentSize = currentSize + 1
    }
    currentBuffer.force()
  }

  private def put(indexHolder: IndexHolder[K], buffer: MappedByteBuffer, capacity: Int): Boolean = {
    val writePosition = buffer.position
    buffer.putInt(indexHolder.bytes.length)
    buffer.put(indexHolder.bytes)

    val (index, isNewEntry) = nextIndex(buffer, capacity, indexHolder.key)
    putIntByIndex(buffer, index, writePosition)
    isNewEntry
  }

  def entries: Iterable[(K, (Int, Int))] = entries(currentBuffer, currentCapacity)

  private def entries(buffer: MappedByteBuffer, capacity: Int): Iterable[(K, (Int, Int))] =
    (0 until capacity)
        .map(i => getIntByIndex(buffer, i))
        .filter(_ != 0)
        .map(p => indexHolderFor(buffer, p))
        .map(h => (h.key, (h.valuePosition, h.valueLength)))

  def size: Int = currentSize

  def contains(key: K): Boolean = findIndex(key).isDefined

  def clear(): Unit = {
    currentCapacity = max(initialCapacity, 1024)
    currentSize = 0
    clear(currentBuffer, currentCapacity)
  }

  private def clear(buffer: MappedByteBuffer, capacity: Int): Unit = {
    buffer.position(0)
    buffer.putInt(capacity)
    (0 until capacity).foreach(_ => buffer.putInt(0))
  }

  def remove(key: K): Unit = {
    findIndex(key) match {
      case Some((i, _)) =>
        putIntByIndex(currentBuffer, i, 0)
        currentSize = currentSize - 1
      case _ => Unit
    }
  }

  private def restore(): Unit = {
    currentBuffer.position(0)
    currentCapacity = currentBuffer.getInt

    val (maxPosition, existingSize) = (0 until currentCapacity).map(i => getIntByIndex(currentBuffer, i)).foldLeft((0, 0)) {
      case ((rmp, rsz), i) =>
        val mp = if (i > rmp) i else rmp
        val sz = rsz + (if (i != 0) 1 else 0)
        (mp, sz)
    }

    if (maxPosition == 0) {
      currentBuffer.position((currentCapacity * 4) + 4)
    } else {
      currentBuffer.position(maxPosition)
      val lengthAtMaxPosition = currentBuffer.getInt
      currentBuffer.position(maxPosition + lengthAtMaxPosition + 4)
    }

    currentSize = existingSize
  }

  @tailrec
  private def nextIndex(buffer: MappedByteBuffer, capacity: Int, key: K, offset: Int = 0): (Int, Boolean) = {
    if (offset > capacity) {
      throw new IllegalStateException("Unable to locate a free index entry")
    }

    val index = ((key.hashCode.abs % capacity) + offset) % capacity
    val indexValue = getIntByIndex(buffer, index)
    if(indexValue == 0L) {
      (index, true)
    } else if (indexHolderFor(buffer, indexValue).key == key) {
      (index, false)
    } else {
      nextIndex(buffer, capacity, key, offset + 1)
    }
  }

  @tailrec
  private def findIndex(key: K, offset: Int = 0): Option[(Int, IndexHolder[K])] = {
    val index = ((key.hashCode.abs % currentCapacity) + offset) % currentCapacity
    val indexValue = getIntByIndex(currentBuffer, index)
    if (indexValue == 0L) {
      None
    } else {
      val indexHolder = indexHolderFor(currentBuffer, indexValue)
      if (indexHolder.key == key) {
        Some((index, indexHolder))
      } else {
        findIndex(key, offset + 1)
      }
    }
  }

  private def indexHolderFor(buffer: MappedByteBuffer, position: Int): IndexHolder[K] = {
    val mark = buffer.position
    buffer.position(position)

    val length = buffer.getInt()
    val data = new Array[Byte](length)
    buffer.get(data)
    buffer.position(mark)
    IndexHolder[K](data)
  }

  private def getIntByIndex(buffer: MappedByteBuffer, index: Int): Int = {
    val mark = buffer.position
    buffer.position((index + 1) * 4)
    try { buffer.getInt } finally { buffer.position(mark) }
  }

  private def putIntByIndex(buffer: MappedByteBuffer, index: Int, n: Int): Unit = {
    val mark = buffer.position
    buffer.position((index + 1) * 4)
    buffer.putInt(n)
    buffer.position(mark)
  }

  private def grow(newCapacity: Int, newFileSize: Long): Unit = {
    val tempBuffer = {
      val fc = FileChannel.open(Files.createTempFile("map-", ".hdg"), CREATE, READ, WRITE, DELETE_ON_CLOSE)
      try { fc.map(READ_WRITE, 0, newFileSize) } finally { fc.close() }
    }

    clear(tempBuffer, newCapacity)
    entries(currentBuffer, currentCapacity).foreach { case (k, (p, l)) => put(IndexHolder(k, p, l), tempBuffer, newCapacity) }

    val newBuffer = {
      val openOptions = Seq(CREATE, READ, WRITE) ++ (if (deleteOnClose) Seq(DELETE_ON_CLOSE) else Seq())
      val fc = FileChannel.open(filename, openOptions:_*)
      try { fc.map(READ_WRITE, 0, newFileSize) } finally { fc.close() }
    }

    clear(newBuffer, newCapacity)
    entries(tempBuffer, newCapacity).foreach { case (k, (p, l)) => put(IndexHolder(k, p, l), newBuffer, newCapacity) }


    currentCapacity = newCapacity
    currentBuffer = newBuffer
    currentBuffer.force()
  }
}

object IndexHolder {
  def apply[K <: JavaSerializable](bytes: Array[Byte]): IndexHolder[K] = bytesToValue(bytes)
}

case class IndexHolder[K <: JavaSerializable](key: K, valuePosition: Int, valueLength: Int) {
  lazy val bytes: Array[Byte] = valueToBytes(this)
}
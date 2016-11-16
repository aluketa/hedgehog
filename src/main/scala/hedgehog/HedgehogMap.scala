package hedgehog

import java.io.{Serializable => JavaSerializable}
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode._
import java.nio.file.{Files, Path}
import java.nio.file.StandardOpenOption._
import java.util
import java.util.AbstractMap.SimpleEntry
import java.util.Map.Entry
import java.util.concurrent.ConcurrentHashMap
import java.util.{Map => JavaMap}

import scala.collection.JavaConversions._
import scala.math.max

class HedgehogMap[K, V <: JavaSerializable](
    filename: Path = Files.createTempFile("map-", ".hdg"),
    initialFileSizeBytes: Long = 0) extends JavaMap[K, V] {

  private val indexAndLengthMap = new ConcurrentHashMap[K, (Int, Int)]()
  private var buffer: MappedByteBuffer = {
    val fc = FileChannel.open(filename, CREATE, READ, WRITE, DELETE_ON_CLOSE)
    val fileSizeBytes = max(initialFileSizeBytes, 1024 * 1024)
    try { fc.map(READ_WRITE, 0, fileSizeBytes) } finally { fc.close() }
  }

  override def put(key: K, value: V): V = {
    try {
      val previousValue = get(key)
      val data = valueToBytes(value)
      if (buffer.capacity < buffer.position + data.length) {
        grow(max(buffer.capacity + data.length, buffer.capacity + (buffer.capacity << 1)))
      }

      val writePosition = buffer.position
      buffer.put(data)
      indexAndLengthMap.put(key, (writePosition, data.length))
      previousValue
    } finally {
      buffer.force()
    }
  }

  override def get(key: scala.Any): V = {
    Option(indexAndLengthMap.get(key)) match {
      case Some((p, l)) => getValueAt(p, l)
      case _ => null.asInstanceOf[V]
    }
  }

  override def containsKey(key: scala.Any): Boolean = indexAndLengthMap.keySet.contains(key)

  override def keySet: util.Set[K] = indexAndLengthMap.keySet

  override def values: util.Collection[V] =
    indexAndLengthMap.keySet.map(k => get(k))

  override def entrySet: util.Set[Entry[K, V]] =
    setAsJavaSet(indexAndLengthMap.keySet.map(k => new SimpleEntry[K, V](k, get(k))))

  override def size: Int = indexAndLengthMap.size

  override def clear(): Unit = {
    indexAndLengthMap.clear()
    buffer.position(0)
  }

  override def remove(key: scala.Any): V = {
    val result = get(key)
    indexAndLengthMap.remove(key)
    result
  }

  override def containsValue(value: scala.Any): Boolean =
    values.contains(value)

  override def isEmpty: Boolean = indexAndLengthMap.isEmpty

  override def putAll(m: JavaMap[_ <: K, _ <: V]): Unit =
    m.foreach { case(k, v) => put(k, v) }

  private def grow(newFileSize: Long): Unit = {
    val tempMap = new HedgehogMap[K, V](Files.createTempFile("map-", ".hdg"), newFileSize)
    indexAndLengthMap.entrySet
      .map(e => (e.getKey, e.getValue))
      .foreach { case (k, (p, l)) => tempMap.put(k, getValueAt(p, l)) }
    val newMap = new HedgehogMap[K, V](filename, newFileSize)
    indexAndLengthMap.entrySet
        .map(e => (e.getKey, e.getValue))
        .foreach { case (k, (p, l)) => newMap.put(k, tempMap.getValueAt(p, l)) }

    buffer = newMap.buffer
    buffer.force()
  }

  private def getValueAt(position: Int, length: Int): V = {
    val mark = buffer.position
    try {
      buffer.position(position)
      val data = new Array[Byte](length)
      buffer.get(data)
      bytesToValue[V](data)
    } finally {
      buffer.position(mark)
    }
  }
}

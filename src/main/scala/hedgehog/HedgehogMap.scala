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
import java.util.{Map => JavaMap}

import scala.collection.JavaConversions._
import scala.math.max
import scala.reflect.ClassTag

object HedgehogMap {
  def createEphemeralMap[K <: JavaSerializable: ClassTag, V <: JavaSerializable]: HedgehogMap[K, V] =
    new HedgehogMap[K, V]

  def createPersistentMap[K <: JavaSerializable: ClassTag, V <: JavaSerializable](
      dataPath: Path,
      name: String): HedgehogMap[K, V] =
    new HedgehogMap[K, V](
      filename = dataPath.resolve(s"map-$name.hdg"),
      indexFilename = dataPath.resolve(s"idx-$name.hdg"),
      deleteOnClose = false)
}

class HedgehogMap[K <: JavaSerializable: ClassTag, V <: JavaSerializable] private (
    filename: Path = Files.createTempFile("map-", ".hdg"),
    indexFilename: Path = Files.createTempFile("idx-", ".hdg"),
    initialFileSizeBytes: Long = 0,
    deleteOnClose: Boolean = true) extends JavaMap[K, V] {

  private val indexStore = new IndexStore[K](filename = indexFilename, deleteOnClose = deleteOnClose)
  private var buffer: MappedByteBuffer = {
    val openOptions = Seq(CREATE, READ, WRITE) ++ (if (deleteOnClose) Seq(DELETE_ON_CLOSE) else Seq())
    val fc = FileChannel.open(filename, openOptions:_*)
    val fileSizeBytes = Seq(initialFileSizeBytes, 1024L * 1024L, fc.size).max
    try { fc.map(READ_WRITE, 0, fileSizeBytes) } finally { fc.close() }
  }

  if (indexStore.size > 0){
    val (maxPosition, lengthAtMaxPosition) =
      indexStore.entries
        .foldLeft((0, 0)) { case ((rp, rl), (_, (p, l))) => if (p >= rp) (p, l) else (rp, rl) }

    buffer.position(maxPosition + lengthAtMaxPosition)
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
      indexStore.put(key, writePosition, data.length)
      previousValue
    } finally {
      buffer.force()
    }
  }

  override def get(key: scala.Any): V = key match {
    case typedKey: K =>
      indexStore.get(typedKey) match {
        case Some((p, l)) => getValueAt(p, l)
        case _ => null.asInstanceOf[V]
      }
    case _ => null.asInstanceOf[V]
  }

  override def containsKey(key: scala.Any): Boolean = key match {
    case typedKey: K => indexStore.contains(typedKey)
    case _ => false
  }

  override def keySet: util.Set[K] = indexStore.entries.map(_._1).toSet[K]

  override def values: util.Collection[V] = indexStore.entries.map { case (_, (p, l)) => getValueAt(p, l) }

  override def entrySet: util.Set[Entry[K, V]] =
    setAsJavaSet(indexStore.entries.map { case (k, (p, l)) => new SimpleEntry[K, V](k, getValueAt(p, l)) }.toSet)

  override def size: Int = indexStore.size

  override def clear(): Unit = {
    indexStore.clear()
    buffer.position(0)
  }

  override def remove(key: scala.Any): V = {
    val result = get(key)
    key match {
      case typedKey: K => indexStore.remove(typedKey)
      case _ => Unit
    }

    result
  }

  override def containsValue(value: scala.Any): Boolean =
    values.contains(value)

  override def isEmpty: Boolean = indexStore.size == 0

  override def putAll(m: JavaMap[_ <: K, _ <: V]): Unit =
    m.foreach { case(k, v) => put(k, v) }

  private def grow(newFileSize: Long): Unit = {
    val tempMap = new HedgehogMap[K, V](Files.createTempFile("map-", ".hdg"), Files.createTempFile("idx-", ".hdg"), newFileSize, deleteOnClose = true)
    indexStore.entries.foreach { case (k, (p, l)) => tempMap.put(k, getValueAt(p, l)) }
    val newMap = new HedgehogMap[K, V](filename, indexFilename, newFileSize, deleteOnClose = deleteOnClose)
    indexStore.entries.foreach { case (k, (p, l)) => newMap.put(k, tempMap.getValueAt(p, l)) }

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

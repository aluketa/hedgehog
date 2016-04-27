package hedgehog

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream, Serializable => JavaSerializable}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.StandardOpenOption._
import java.util
import java.util.AbstractMap.SimpleEntry
import java.util.Map.Entry
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import java.util.function.LongUnaryOperator
import java.util.{Map => JavaMap}
import scala.collection.JavaConversions._

class HedgehogMap[K, V <: JavaSerializable] extends JavaMap[K, V] {

  private val indexAndLengthMap = new ConcurrentHashMap[K, (Long, Int)]()
  private val fileChannel =
    FileChannel
        .open(Files.createTempFile("map-", ".hdg"), CREATE, TRUNCATE_EXISTING,  READ, WRITE, DELETE_ON_CLOSE)
  private val position = new AtomicLong(0L)

  override def put(key: K, value: V): V = {
    val data = valueToBytes(value)
    val writePosition = position.getAndUpdate(new LongUnaryOperator {
      override def applyAsLong(operand: Long): Long = operand + data.length
    })

    fileChannel.position(writePosition)
    fileChannel.write(ByteBuffer.wrap(data))
    indexAndLengthMap.put(key, (writePosition, data.length))
    value
  }

  private def valueToBytes(value: V): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    try {
      new ObjectOutputStream(out).writeObject(value)
      out.toByteArray
    } finally {
      out.close()
    }
  }

  override def get(key: scala.Any): V = {
    Option(indexAndLengthMap.get(key)) match {
      case Some((p, l)) =>
        val data = new Array[Byte](l)
        fileChannel.position(p)
        fileChannel.read(ByteBuffer.wrap(data))
        bytesToValue(data)
      case _ => null.asInstanceOf[V]
    }
  }

  private def bytesToValue(bytes: Array[Byte]): V = {
    val in = new ByteArrayInputStream(bytes)
    try {
      new ObjectInputStream(in).readObject.asInstanceOf[V]
    } finally {
      in.close()
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
    position.set(0)
    fileChannel.truncate(0)
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
}

package hedgehog

import java.io.{Serializable => JavaSerializable}
import java.nio.file.{Files, Path, Paths}
import java.util
import java.util.AbstractMap.SimpleEntry
import java.util.Map.Entry
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.ReentrantLock
import java.util.{UUID, Map => JavaMap}

import scala.collection.JavaConversions._
import scala.math.max
import scala.reflect.ClassTag

object HedgehogMap {
  def createEphemeralMap[K <: JavaSerializable: ClassTag, V <: JavaSerializable: ClassTag](concurrencyFactor: Int = 1): HedgehogMap[K, V] =
    new HedgehogMap[K, V](
      dataPath = Paths.get(System.getProperty("java.io.tmpdir")),
      name = UUID.randomUUID.toString,
      concurrencyFactor = concurrencyFactor)

  def createPersistentMap[K <: JavaSerializable: ClassTag, V <: JavaSerializable: ClassTag](
      dataPath: Path,
      name: String,
      concurrencyFactor: Int = 1): HedgehogMap[K, V] =
    new HedgehogMap[K, V](dataPath = dataPath, name = name, isPersistent = true, concurrencyFactor = concurrencyFactor)
}

class HedgehogMap[K <: JavaSerializable: ClassTag, V <: JavaSerializable: ClassTag] private (
    dataPath: Path,
    name: String,
    initialFileSizeBytes: Long = 0,
    isPersistent: Boolean = false,
    concurrencyFactor: Int = 1) extends JavaMap[K, V] {

  private val shards: Map[Int, Shard] = (0 until concurrencyFactor).map(i => (i, createShard(i))).toMap

  private def createShard(n: Int): Shard = {
    val shardSuffix: String = if (n == 0) "" else s"-$n"
    val filename = dataPath.resolve(s"map-$name$shardSuffix.hdg")
    val indexFilename = dataPath.resolve(s"idx-$name$shardSuffix.hdg")
    val indexStore = new IndexStore[K](filename = indexFilename, isPersistent = isPersistent)
    val buffer = new LargeMappedByteBuffer(filename, initialFileSizeBytes, isPersistent)

    if (indexStore.size > 0){
      val (maxPosition, lengthAtMaxPosition) =
        indexStore.entries
            .foldLeft((0L, 0)) { case ((rp, rl), (_, (p, l))) => if (p >= rp) (p, l) else (rp, rl) }

      buffer.position(maxPosition + lengthAtMaxPosition)
    }

    Shard(new ReentrantLock, filename, indexStore, new AtomicReference(buffer))
  }

  private def shardForKey(key: K): Shard = shards(key.hashCode.abs % shards.size)

  private def atomically[T](func: (Seq[(IndexStore[K], AtomicReference[LargeMappedByteBuffer], Path)]) => T): T = {
    val allLocks = (0 until shards.size).map(i => shards(i).lock)
    allLocks.foreach(_.lock())
    try {
      func(shards.values.map(v => (v.indexStore, v.buffer, v.filename)).toSeq)
    } finally {
      allLocks.reverse.foreach(_.unlock())
    }
  }

  override def put(key: K, value: V): V = {
    shardForKey(key).atomically {
      (indexStore, buffer, filename) =>
        val previousValue = get(key)
        val data = valueToBytes(value)
        if (buffer.get.capacity < buffer.get.position + data.length) {
          grow(max(buffer.get.capacity + data.length, buffer.get.capacity * 2L), indexStore, buffer, filename)
        }

        val writePosition = buffer.get.position
        buffer.get.put(data)
        indexStore.put(key, writePosition, data.length)
        previousValue
    }
  }

  override def get(key: scala.Any): V = key match {
    case typedKey: K =>
      shardForKey(typedKey).atomically {
        (indexStore, buffer, _) =>
          indexStore.get(typedKey) match {
            case Some((p, l)) => getValueAt(buffer.get, p, l)
            case _ => null.asInstanceOf[V]
          }
      }
    case _ => null.asInstanceOf[V]
  }

  override def containsKey(key: scala.Any): Boolean = key match {
    case typedKey: K => shardForKey(typedKey).atomically( (indexStore, _, _) => indexStore.contains(typedKey))
    case _ => false
  }

  override def keySet: util.Set[K] =
    atomically(indexStoresAndBuffers =>
      indexStoresAndBuffers.map(_._1).flatMap(_.entries.map(_._1)).toSet[K])

  override def values: util.Collection[V] =
    atomically(indexStoresAndBuffers =>
      indexStoresAndBuffers.flatMap(x => x._1.entries.map { case (_, (p, l)) => getValueAt(x._2.get, p, l) }))

  override def entrySet: util.Set[Entry[K, V]] =
    atomically(indexStoresAndBuffers =>
      setAsJavaSet(indexStoresAndBuffers.flatMap(x =>
        x._1.entries.map { case (k, (p, l)) =>
          new SimpleEntry[K, V](k, getValueAt(x._2.get, p, l)) }).toSet))

  override def size: Int = atomically(indexStoresAndBuffers => indexStoresAndBuffers.map(_._1.size).sum)

  override def clear(): Unit =
    atomically(_.foreach {
      case (indexStore, buffer, _) =>
        indexStore.clear()
        buffer.get.position(0)
    })

  override def remove(key: scala.Any): V = {
    val result = get(key)
    key match {
      case typedKey: K => shardForKey(typedKey).atomically((indexStore, _, _) => indexStore.remove(typedKey))
      case _ => Unit
    }

    result
  }

  override def containsValue(value: scala.Any): Boolean =
    values.contains(value)

  override def isEmpty: Boolean = atomically(indexStoresAndBuffers => indexStoresAndBuffers.forall(_._1.size == 0))

  override def putAll(m: JavaMap[_ <: K, _ <: V]): Unit =
    m.foreach { case(k, v) => put(k, v) }

  def force(): Unit =
    atomically(_.foreach {
      case (indexStore, buffer, _) =>
        indexStore.force()
        buffer.get.force()
    })

  def compact(): Unit = atomically(_.foreach(x => compact(x._1, x._2, x._3)))

  private def compact(indexStore: IndexStore[K], buffer: AtomicReference[LargeMappedByteBuffer], filename: Path): Unit = {
    val compactSize: Long = indexStore.entries.map { case (_, (_, l)) => l.toLong }.sum
    val tempBuffer = new LargeMappedByteBuffer(Files.createTempFile("map-", ".hdg"), compactSize, isPersistent = false)

    tempBuffer.position(0)
    val tempIndexStore = new IndexStore[K](isPersistent = false)

    indexStore.entries.foreach {
      case (key, (p, l)) =>
        val data = new Array[Byte](l)
        buffer.get.position(p)
        buffer.get.get(data)

        val writePosition = tempBuffer.position
        tempBuffer.put(data)
        tempIndexStore.put(key, writePosition, l)
    }

    if (isPersistent) {
      Files.delete(filename)
    }

    val newBuffer = new LargeMappedByteBuffer(filename, compactSize, isPersistent)
    copyBuffers(tempIndexStore, tempBuffer, newBuffer)
    buffer.set(newBuffer)
    indexStore.compact(overrideSourceIndex = Some(tempIndexStore))
  }

  private def grow(newFileSize: Long, indexStore: IndexStore[K], buffer: AtomicReference[LargeMappedByteBuffer], filename: Path): Unit = {
    val writePosition = buffer.get.position
    val newBuffer: LargeMappedByteBuffer = new LargeMappedByteBuffer(filename, newFileSize, isPersistent)
    if (isPersistent) {
      buffer.get.force()
    } else {
      copyBuffers(indexStore, buffer.get, newBuffer)
    }

    buffer.set(newBuffer)
    buffer.get.position(writePosition)
  }

  private def copyBuffers(indexStore: IndexStore[K], src: LargeMappedByteBuffer, dest: LargeMappedByteBuffer): Unit = {
    indexStore.entries.foreach { case (_, (p, l)) =>
      val data = new Array[Byte](l)
      src.position(p)
      src.get(data)
      dest.position(p)
      dest.put(data)
    }
  }

  private def getValueAt(buffer: LargeMappedByteBuffer, position: Long, length: Int): V = {
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

  private case class Shard(lock: ReentrantLock, filename: Path, indexStore: IndexStore[K], buffer: AtomicReference[LargeMappedByteBuffer]) {
    def atomically[T](func: (IndexStore[K], AtomicReference[LargeMappedByteBuffer], Path) => T): T = {
      lock.lock()
      try {
        func(indexStore, buffer, filename)
      } finally {
        lock.unlock()
      }
    }
  }
}

package hedgehog

import java.io.{Serializable => JavaSerializable}
import java.nio.file.{Files, Path, Paths}
import java.util
import java.util.AbstractMap.SimpleEntry
import java.util.Map.Entry
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.ReentrantLock
import java.util.{Objects, UUID, Map => JavaMap}
import java.util.concurrent.ConcurrentMap

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
    concurrencyFactor: Int = 1) extends ConcurrentMap[K, V] {

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

  private def atomically[T](func: Seq[Shard] => T): T = {
    val allLocks = (0 until shards.size).map(i => shards(i).lock)
    allLocks.foreach(_.lock())
    try {
      func(shards.values.toSeq)
    } finally {
      allLocks.reverse.foreach(_.unlock())
    }
  }

  override def put(key: K, value: V): V = {
    shardForKey(key).atomically {
      shard =>
        val previousValue = get(key)
        val data = valueToBytes(value)
        if (shard.buffer.get.capacity < shard.buffer.get.position + data.length) {
          grow(max(shard.buffer.get.capacity + data.length, shard.buffer.get.capacity * 2L), shard)
        }

        val writePosition = shard.buffer.get.position
        shard.buffer.get.put(data)
        shard.indexStore.put(key, writePosition, data.length)
        previousValue
    }
  }

  override def get(key: scala.Any): V = key match {
    case typedKey: K =>
      shardForKey(typedKey).atomically {
        shard =>
          shard.indexStore.get(typedKey) match {
            case Some((p, l)) => getValueAt(shard.buffer.get, p, l)
            case _ => null.asInstanceOf[V]
          }
      }
    case _ => null.asInstanceOf[V]
  }

  override def containsKey(key: scala.Any): Boolean = key match {
    case typedKey: K => shardForKey(typedKey).atomically(shard => shard.indexStore.contains(typedKey))
    case _ => false
  }

  override def keySet: util.Set[K] =
    atomically(_.map(_.indexStore).flatMap(_.entries.map(_._1)).toSet[K])

  override def values: util.Collection[V] =
    atomically(_.flatMap(shard =>
        shard.indexStore.entries.map { case (_, (p, l)) => getValueAt(shard.buffer.get, p, l) }))

  override def entrySet: util.Set[Entry[K, V]] =
    atomically(shards =>
      setAsJavaSet(shards.flatMap(shard =>
        shard.indexStore.entries.map { case (k, (p, l)) =>
          new SimpleEntry[K, V](k, getValueAt(shard.buffer.get, p, l)) }).toSet))

  override def size: Int = atomically(_.map(_.indexStore.size).sum)

  override def clear(): Unit =
    atomically(_.foreach {
      shard =>
        shard.indexStore.clear()
        shard.buffer.get.position(0)
    })

  override def remove(key: scala.Any): V = {
    val result = get(key)
    key match {
      case typedKey: K => shardForKey(typedKey).atomically(shard => shard.indexStore.remove(typedKey))
      case _ => Unit
    }

    result
  }

  override def containsValue(value: scala.Any): Boolean =
    values.contains(value)

  override def isEmpty: Boolean = atomically(_.forall(_.indexStore.size == 0))

  override def putAll(m: JavaMap[_ <: K, _ <: V]): Unit =
    m.foreach { case(k, v) => put(k, v) }

  override def replace(key: K, oldValue: V, newValue: V): Boolean =
    shardForKey(key).atomically { _ =>
      if (this.containsKey(key) && Objects.equals(this.get(key), oldValue)) {
        this.put(key, newValue)
        true
      } else {
        false
      }
    }

  override def replace(key: K, value: V): V = {
    shardForKey(key).atomically { _ =>
      if (this.containsKey(key)) {
        this.put(key, value)
      } else {
        null.asInstanceOf[V]
      }
    }
  }

  override def remove(key: scala.Any, value: scala.Any): Boolean = key match {
    case typedKey: K =>
      shardForKey(typedKey).atomically { _ =>
        if (this.containsKey(typedKey) && Objects.equals(this.get(typedKey), value)) {
          this.remove(key)
          true
        } else {
          false
        }
      }
    case _ => false
  }

  override def putIfAbsent(key: K, value: V): V = {
    shardForKey(key).atomically { _ =>
      if (!this.containsKey(key)) {
        this.put(key, value)
      } else {
        this.get(key)
      }
    }
  }

  def force(): Unit =
    atomically(_.foreach {
      shard =>
        shard.indexStore.force()
        shard.buffer.get.force()
    })

  def compact(): Unit = atomically(_.foreach(compact))

  private def compact(shard: Shard): Unit = {
    val compactSize: Long = shard.indexStore.entries.map { case (_, (_, l)) => l.toLong }.sum
    val tempBuffer = new LargeMappedByteBuffer(Files.createTempFile("map-", ".hdg"), compactSize, isPersistent = false)

    tempBuffer.position(0)
    val tempIndexStore = new IndexStore[K](isPersistent = false)

    shard.indexStore.entries.foreach {
      case (key, (p, l)) =>
        val data = new Array[Byte](l)
        shard.buffer.get.position(p)
        shard.buffer.get.get(data)

        val writePosition = tempBuffer.position
        tempBuffer.put(data)
        tempIndexStore.put(key, writePosition, l)
    }

    if (isPersistent) {
      Files.delete(shard.filename)
    }

    val newBuffer = new LargeMappedByteBuffer(shard.filename, compactSize, isPersistent)
    copyBuffers(tempIndexStore, tempBuffer, newBuffer)
    shard.buffer.set(newBuffer)
    shard.indexStore.compact(overrideSourceIndex = Some(tempIndexStore))
  }

  private def grow(newFileSize: Long, shard: Shard): Unit = {
    val writePosition = shard.buffer.get.position
    val newBuffer: LargeMappedByteBuffer = new LargeMappedByteBuffer(shard.filename, newFileSize, isPersistent)
    if (isPersistent) {
      shard.buffer.get.force()
    } else {
      copyBuffers(shard.indexStore, shard.buffer.get, newBuffer)
    }

    shard.buffer.set(newBuffer)
    shard.buffer.get.position(writePosition)
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
    def atomically[T](func: Shard => T): T = {
      lock.lock()
      try {
        func(this)
      } finally {
        lock.unlock()
      }
    }
  }
}

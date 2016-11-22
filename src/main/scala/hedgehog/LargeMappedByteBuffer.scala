package hedgehog

import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.nio.file.StandardOpenOption._
import FileChannel.MapMode.READ_WRITE

import scala.math.{ceil, floor}

private[hedgehog] class LargeMappedByteBuffer(
    filename: Path,
    fileSizeBytes: Long,
    isPersistent: Boolean,
    maxShardSize: Int = Int.MaxValue) {

  private val shards: Seq[MappedByteBuffer] = {
    val openOptions = Seq(CREATE, READ, WRITE) ++ (if (!isPersistent) Seq(DELETE_ON_CLOSE) else Seq())
    val fc = FileChannel.open(filename, openOptions:_*)
    val targetFileSize: Long = Seq(fileSizeBytes, 1024L * 1024L, fc.size).max

    val shardCount: Int = ceil(targetFileSize.toDouble / maxShardSize.toDouble).toInt
    val lastShardSize: Long = targetFileSize - ((shardCount - 1).toLong * maxShardSize)

    try {
      (0 until shardCount - 1).map { i =>
        val shardSize = Seq(targetFileSize, maxShardSize.toLong).min
        fc.map(READ_WRITE, i.toLong * maxShardSize, shardSize)
      } :+
          fc.map(READ_WRITE, (shardCount - 1).toLong * maxShardSize, lastShardSize)
    } finally {
      fc.close()
    }
  }

  private var currentPosition: Long = 0L

  def position: Long = currentPosition

  def position(newPosition: Long): Unit = currentPosition = newPosition

  def force(): Unit = shards.foreach(_.force())

  def capacity: Long = shards.map(_.capacity.toLong).sum

  def put(data: Array[Byte]): Unit = {
    val currentShard: Int = floor(currentPosition.toDouble / maxShardSize.toDouble).toInt
    shards(currentShard).position((currentPosition - (currentShard * maxShardSize)).toInt)
    val (dataForShard, remainingData) = data.splitAt(shards(currentShard).capacity - shards(currentShard).position)
    shards(currentShard).put(dataForShard)
    currentPosition = currentPosition + dataForShard.length

    if (remainingData.nonEmpty) {
      put(remainingData)
    }
  }

  def get(data: Array[Byte]): Unit = {
    val currentShard: Int = floor(currentPosition.toDouble / maxShardSize.toDouble).toInt
    shards(currentShard).position((currentPosition - (currentShard * maxShardSize)).toInt)
    val (dataForShard, remainingData) = data.splitAt(shards(currentShard).capacity - shards(currentShard).position)
    shards(currentShard).get(dataForShard)
    currentPosition = currentPosition + dataForShard.length

    if (remainingData.nonEmpty) {
      get(remainingData)
    }
    mergeByteArrays(dataForShard, remainingData, data)
  }

  private def mergeByteArrays(first: Array[Byte], second: Array[Byte], target: Array[Byte]): Unit = {
    Array.copy(first, 0, target, 0, first.length)
    Array.copy(second, 0, target, first.length, second.length)
  }
}

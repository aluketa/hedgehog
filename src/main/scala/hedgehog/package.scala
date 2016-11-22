import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.io.{Serializable => JavaSerializable}

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.reflect._

package object hedgehog {
  private[hedgehog] def bytesToValue[T <: JavaSerializable: ClassTag](bytes: Array[Byte]): T = {
    if (classTag[T].runtimeClass == classOf[String]) {
      new String(bytes).asInstanceOf[T]
    } else {
      val in = new ByteArrayInputStream(bytes)
      try {
        new ObjectInputStream(in).readObject.asInstanceOf[T]
      } finally {
        in.close()
      }
    }
  }

  private[hedgehog] def valueToBytes[T <: JavaSerializable: ClassTag](value: T): Array[Byte] =
    value match {
      case str: String => str.getBytes
      case _ =>
        val out = new ByteArrayOutputStream()
        try {
          new ObjectOutputStream(out).writeObject(value)
          out.toByteArray
        } finally {
          out.close()
        }
    }

  private[hedgehog] object Timer {
    private val times = new mutable.ArrayBuffer[Long]

    def time[T](func: => T): T = {
      val st = System.currentTimeMillis
      try {
        func
      } finally {
        times += (System.currentTimeMillis - st)
      }
    }

    def printStats(): Unit = {
      if (times.nonEmpty) {
        val mean = times.sum.toDouble / times.size.toDouble
        val std = math.sqrt(times.map(t => math.pow(t - mean, 2)).sum / times.size)
        println(s"max: ${times.max}, min: ${times.min}, avg: $mean, std: $std")
      } else {
        println("No timings!")
      }
    }

    def average: Double = times.sum.toDouble / times.size.toDouble

    def reset(): Unit = times.clear()
  }
}

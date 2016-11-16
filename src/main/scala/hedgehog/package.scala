import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

package object hedgehog {
  def bytesToValue[T](bytes: Array[Byte]): T = {
    val in = new ByteArrayInputStream(bytes)
    try {
      new ObjectInputStream(in).readObject.asInstanceOf[T]
    } finally {
      in.close()
    }
  }

  def valueToBytes(value: AnyRef): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    try {
      new ObjectOutputStream(out).writeObject(this)
      out.toByteArray
    } finally {
      out.close()
    }
  }
}

package common

import java.io.{BufferedInputStream, FileInputStream}
import java.nio.file.{Files, Paths}

object RecordIO {
  val RecordSize = 100
  val KeySize    = 10

  // 10바이트 키의 unsigned-lexicographic 비교
  def compareKeys(a: Array[Byte], b: Array[Byte]): Int = {
    var i = 0
    while (i < KeySize) {
      val x = a(i) & 0xFF
      val y = b(i) & 0xFF
      if (x != y) return x - y
      i += 1
    }
    0
  }

  // 100바이트 레코드를 스트리밍하며 (key, value) 콜백 호출
  def streamRecords(path: String)(f: (Array[Byte], Array[Byte]) => Unit): Unit = {
    val in = new BufferedInputStream(new FileInputStream(path))
    try {
      val buf = new Array[Byte](RecordSize)
      var n   = in.read(buf)
      while (n == RecordSize) {
        val key   = java.util.Arrays.copyOfRange(buf, 0, KeySize)
        val value = java.util.Arrays.copyOfRange(buf, KeySize, RecordSize)
        f(key, value)
        n = in.read(buf)
      }
    } finally in.close()
  }

  def exists(path: String): Boolean =
    Files.exists(Paths.get(path))
}

package common

object Sampling {
  /** every-N 방식 균등 샘플링: key만 수집 (ArrayBuffer 미사용) */
  def uniformEveryN(files: Seq[String], everyN: Int): Array[Array[Byte]] = {
    require(everyN > 0, s"everyN must be > 0, but was $everyN")

    val builder = Array.newBuilder[Array[Byte]] // 최종적으로 Array[Array[Byte]] 생성
    var i = 0

    files.foreach { p =>
      if (RecordIO.exists(p)) {
        RecordIO.streamRecords(p) { (k, _) =>
          if (i % everyN == 0) {
            val keyCopy = new Array[Byte](RecordIO.KeySize)
            System.arraycopy(k, 0, keyCopy, 0, RecordIO.KeySize)
            builder += keyCopy
          }
          i += 1
        }
      }
    }
    builder.result()
  }
}

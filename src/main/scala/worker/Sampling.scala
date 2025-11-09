package worker

import common.RecordIO

object Sampling {

  /** 
    * 각 input 파일에서 step번째마다 key를 하나씩 샘플링해서 모은다.
    * 파일이 없으면 경고만 출력하고 건너뜀.
    */
  def sampleKeysEveryN(inputPaths: Seq[String], step: Int): Vector[Array[Byte]] = {
    require(step > 0, "step must be > 0")

    val builder = Vector.newBuilder[Array[Byte]]

    inputPaths.foreach { path =>
      if (!RecordIO.exists(path)) {
        Console.err.println(s"[Sampling] WARNING: input file not found: $path (skipped)")
      } else {
        var idx = 0L
        RecordIO.streamRecords(path) { (key, _value) =>
          idx += 1
          if (idx % step == 0) {
            // key는 10바이트 배열, 복사해서 보관
            builder += key.clone()
          }
        }
      }
    }

    builder.result()
  }
}

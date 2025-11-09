package common

import java.io.File

object Sampling {
  /** every-N 방식 균등 샘플링: key만 수집 (ArrayBuffer 미사용) */
  def uniformEveryN(paths: Seq[String], everyN: Int): Array[Array[Byte]] = {
    require(everyN > 0, s"everyN must be > 0, but was $everyN")

    val builder = Array.newBuilder[Array[Byte]]
    var i = 0

    // 각 경로를 파일 리스트로 확장
    val allFiles = paths.flatMap { path =>
      val f = new File(path)
      if (f.isDirectory) {
        // 디렉토리면 안의 .dat 파일들 찾기
        f.listFiles()
          .filter(_.getName.endsWith(".dat"))
          .map(_.getPath)
          .toSeq
      } else if (f.isFile) {
        // 파일이면 그대로
        Seq(path)
      } else {
        Console.err.println(s"[Sampling] WARNING: not found: $path (skipped)")
        Seq.empty
      }
    }

    // 모든 파일에서 샘플링
    allFiles.foreach { filePath =>
      if (RecordIO.exists(filePath)) {
        RecordIO.streamRecords(filePath) { (k, _) =>
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
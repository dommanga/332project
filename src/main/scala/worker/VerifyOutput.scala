package worker

import java.io.File
import common.RecordIO

/**
 * Week 7 – Sangwon
 * 분산 정렬이 끝난 뒤 결과를 검증하기 위한 유틸리티.
 *
 * 사용 예시 (sbt):
 *   sbt "runMain worker.VerifyOutput ./out 10 1000000"
 *
 *  - outputDir: partition 파일들이 있는 디렉토리
 *  - expectedPartitions: 기대하는 partition 개수 (partition.0 ~ partition.N-1)
 *  - expectedRecords: 전체 레코드 개수 (선택적, 0이면 건너뜀)
 */
object VerifyOutput {

  /** outputDir 아래 partition.N 파일들이 모두 존재하는지 확인 */
  def verifyAllPartitionsPresent(outputDir: String, expectedPartitions: Int): Boolean = {
    println(s"[Verify] Checking partition files in: $outputDir")
    println(s"[Verify] Expecting partition.0 ~ partition.${expectedPartitions - 1}")

    val dir = new File(outputDir)
    if (!dir.exists() || !dir.isDirectory) {
      Console.err.println(s"[Verify] ❌ Output directory does not exist or is not a directory: $outputDir")
      return false
    }

    val files = (0 until expectedPartitions).map { i =>
      new File(dir, s"partition.$i")
    }

    val missing = files.filterNot(_.exists())

    if (missing.nonEmpty) {
      Console.err.println(s"[Verify] ❌ Missing partition files:")
      missing.foreach(f => Console.err.println(s"  - ${f.getName}"))
      false
    } else {
      println(s"[Verify] ✅ All $expectedPartitions partition files are present.")
      true
    }
  }

  /** 전체 레코드 수가 expectedRecords 와 일치하는지 확인 (expectedRecords == 0이면 스킵) */
  def verifyRecordCount(outputDir: String, expectedRecords: Long): Boolean = {
    if (expectedRecords <= 0) {
      println("[Verify] (skip) expectedRecords <= 0 → record count check skipped.")
      return true
    }

    println(s"[Verify] Checking total record count (expected = $expectedRecords)")

    val dir = new File(outputDir)
    val partitionFiles = dir.listFiles()
      .filter(f => f.isFile && f.getName.startsWith("partition."))

    val totalRecords: Long = partitionFiles.map { f =>
      f.length() / RecordIO.RecordSize
    }.sum

    if (totalRecords == expectedRecords) {
      println(s"[Verify] ✅ Record count matches: $totalRecords")
      true
    } else {
      Console.err.println(s"[Verify] ❌ Record count mismatch: expected=$expectedRecords, actual=$totalRecords")
      false
    }
  }

  /** 각 partition 내부, 그리고 partition 간 전체 정렬 순서가 올바른지 확인 */
  def verifySortOrder(outputDir: String): Boolean = {
    println("[Verify] Checking global sort order (within + across partitions)")

    val dir = new File(outputDir)
    val partitionFiles = dir.listFiles()
      .filter(f => f.isFile && f.getName.startsWith("partition."))
      .sortBy { f =>
        // "partition.X" 에서 X 부분을 정수로 파싱
        val idxStr = f.getName.stripPrefix("partition.")
        idxStr.toIntOption.getOrElse(Int.MaxValue)
      }

    var prevMaxKey: Array[Byte] = null
    var allSorted = true

    partitionFiles.foreach { file =>
      println(s"[Verify]  - Checking ${file.getName}")

      val records = readRecords(file)

      // (1) partition 내부 정렬 확인
      val withinSorted = records.sliding(2).forall {
        case Seq(r1, r2) =>
          val k1 = extractKey(r1)
          val k2 = extractKey(r2)
          RecordIO.compareKeys(k1, k2) <= 0
        case _ => true
      }

      if (!withinSorted) {
        Console.err.println(s"[Verify] ❌ ${file.getName} is NOT sorted internally")
        allSorted = false
      }

      // (2) 이전 partition 의 마지막 key <= 이번 partition 첫 key 인지 확인
      if (prevMaxKey != null && records.nonEmpty) {
        val firstKey = extractKey(records.head)
        if (RecordIO.compareKeys(prevMaxKey, firstKey) > 0) {
          Console.err.println(
            s"[Verify] ❌ Sort order violation around ${file.getName} " +
              s"(prevMaxKey > firstKey)"
          )
          allSorted = false
        }
      }

      if (records.nonEmpty) {
        prevMaxKey = extractKey(records.last)
      }
    }

    if (allSorted) {
      println("[Verify] ✅ Global sort order verified.")
    }

    allSorted
  }

  /** 파일에서 100B 레코드들을 전부 읽어서 Seq[Array[Byte]] 로 반환 */
  private def readRecords(file: File): Seq[Array[Byte]] = {
    val bytes = java.nio.file.Files.readAllBytes(file.toPath)
    val recSize = RecordIO.RecordSize
    val count = bytes.length / recSize

    (0 until count).map { i =>
      java.util.Arrays.copyOfRange(bytes, i * recSize, (i + 1) * recSize)
    }
  }

  /** 레코드에서 key(앞 10바이트)만 잘라서 반환 */
  private def extractKey(record: Array[Byte]): Array[Byte] =
    java.util.Arrays.copyOfRange(record, 0, RecordIO.KeySize)

  /** 세 가지 검증을 한 번에 수행하는 헬퍼 */
  def runFullVerification(
      outputDir: String,
      expectedPartitions: Int,
      expectedRecords: Long
  ): Boolean = {
    println("\n" + "=" * 60)
    println("   Output Verification")
    println("=" * 60)

    val checks = Seq(
      verifyAllPartitionsPresent(outputDir, expectedPartitions),
      verifyRecordCount(outputDir, expectedRecords),
      verifySortOrder(outputDir)
    )

    val allOk = checks.forall(identity)

    println("=" * 60)
    if (allOk) {
      println("✅✅✅  ALL VERIFICATION CHECKS PASSED")
    } else {
      println("❌❌❌  VERIFICATION FAILED")
    }
    println("=" * 60 + "\n")

    allOk
  }

  /** sbt runMain entrypoint */
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("Usage: VerifyOutput <outputDir> <numPartitions> [expectedRecords]")
      System.exit(1)
    }

    val outputDir = args(0)
    val numPartitions = args(1).toInt
    val expectedRecords =
      if (args.length >= 3) args(2).toLong else 0L

    runFullVerification(outputDir, numPartitions, expectedRecords)
  }
}

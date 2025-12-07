package entry

object Main {
  def usage(): Unit = {
    System.err.println(
      """Usage:
        |  java -jar dist-sort.jar master <num_workers>
        |  java -jar dist-sort.jar worker <master_ip:port> -I <input_dir> -O <output_dir>
        |
        |Examples:
        |  java -jar dist-sort.jar master 3
        |  java -jar dist-sort.jar worker 2.2.2.254:35939 -I /dataset/small -O /home/orange/out
        |""".stripMargin
    )
  }

  def main(args: Array[String]): Unit = {
    if (args.isEmpty) {
      usage()
      System.exit(1)
    }

    args(0) match {
      case "master" =>
        master.MasterServer.main(args.slice(1, args.length))

      case "worker" =>
        worker.WorkerClient.main(args.slice(1, args.length))

      case other =>
        System.err.println(s"[ERROR] Unknown mode: '$other' (expected 'master' or 'worker')\n")
        usage()
        System.exit(1)
    }
  }
}
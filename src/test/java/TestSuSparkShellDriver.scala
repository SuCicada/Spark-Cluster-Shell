import sucicada.susparkshell.SuSparkShellDriver

object TestSuSparkShellDriver {
    def main(args: Array[String]): Unit = {
        SuSparkShellDriver.main(Array(
            "--su.driver.port", "9999",
            "--su.spark-submit.mode", "CLASS",
            "--name", "Spark Cluster Shell",
            "--master", "local"
        ))
    }
}

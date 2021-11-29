import org.apache.spark.repl.SuSparkRepl
import org.apache.spark.sql.SparkSession

object Main {
    def main(args: Array[String]): Unit = {
        System.setProperty("scala.usejavacp", "true")
        scala.tools.nsc.MainGenericRunner.main(Array())
    }
}

object TestSpark {
    def main(args: Array[String]): Unit = {
        SparkSession.builder()
            .master("local")
            .enableHiveSupport
            .getOrCreate
            .sql("show databases")
            .show
    }
}


object Submit {
    def main(args: Array[String]): Unit = {
        System.setProperty("spark.master", "local")
        //        System.setProperty("HADOOP_CONF_DIR", "C:\\Users\\ZQ091606\\Documents\\PROGRAM\\GitLab\\utils\\java\\spark\\src\\main\\resources")
        System.setProperty("scala.usejavacp", "true")
        val className = org.apache.spark.repl.SuSparkRepl.getClass.getName
        println("className ", className)
        org.apache.spark.deploy.SparkSubmit.main(Array(
            "--class", "org.apache.spark.repl.SuSparkRepl",
            "--name", "SuSpark Shell",
            //            "--master", "local",
            "--master", "yarn",
            "--deploy-mode", "cluster",
            "--queue", "saas_prds_audience_routine",
            //            "--help",
            "C:\\Users\\ZQ091606\\Documents\\PROGRAM\\GitLab\\utils\\java\\spark\\target\\spark-cluster-shell.jar",
            "--su.spark-submit.mode", "SHELL",
            "--su.driver.port", "9999"
        ))
    }
}
//export SPARK_SUBMIT_OPTS="$SPARK_SUBMIT_OPTS -Dscala.usejavacp=true"

//

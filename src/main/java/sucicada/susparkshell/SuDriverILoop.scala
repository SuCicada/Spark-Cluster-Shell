package sucicada.susparkshell

import java.io.BufferedReader
import scala.tools.nsc.interpreter.{ILoop, JPrintWriter}
import scala.util.Properties.{javaVersion, javaVmName, versionString}

class SuDriverILoop(in0: Option[BufferedReader], out: JPrintWriter)
    extends ILoop(in0, out) {
    def this(in0: BufferedReader, out: JPrintWriter) = this(Some(in0), out)

    def this() = this(None, new JPrintWriter(Console.out, true))

    override def printWelcome() {
        import org.apache.spark.SPARK_VERSION
        val suspark_shell_version = "socket-cluster"
        echo(
            """Welcome to
    ____     ____              __     ______       ____
   / __/_ __/ __/__  ___ _____/ /__  / __/ /  ___ / / /
  _\ \/ // /\ \/ _ \/ _ `/ __/  '_/ _\ \/ _ \/ -_) / /
 /___/\_,_/___/ .__/\_,_/_/ /_/\_\ /___/_//_/\__/_/_/  version %s
         """.format(suspark_shell_version))
        val welcomeMsg = "Using Spark %s (Scala %s (%s, Java %s))".format(
            SPARK_VERSION, versionString, javaVmName, javaVersion)
        echo(welcomeMsg)
        echo("Type in expressions to have them evaluated.")
        echo("Type :help for more information.")
    }
}

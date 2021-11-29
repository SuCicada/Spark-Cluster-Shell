package org.apache.spark.repl


import org.apache.spark.internal.config.UI_SHOW_CONSOLE_PROGRESS
import sucicada.susparkshell.FlagConstant.{SPARK_REPL_IGNORE_PROMPT, SU_REPL_PROMPT}

import java.io.{BufferedReader, InputStreamReader, PrintStream, PrintWriter}
import java.net.Socket
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.sys.Prop

/**
 * 进度条看 [[ org.apache.spark.ui.ConsoleProgressBar]]
 */
class SuSparkRepl

object SuSparkRepl {
    var host: String = _
    var port: Int = _

    def parseArgs(args: Array[String]) = {
        var i = 0
        val newArgs = ArrayBuffer[String]()
        while (i < args.length) {
            args(i) match {
                case "--su.driver.host" =>
                    host = args(i + 1); i += 1
                case "--su.driver.port" =>
                    port = args(i + 1).toInt; i += 1
                case s =>
                    newArgs += s
            }
            i += 1
        }
        newArgs.toArray
    }

    val reTrySum = 3


    def main(args: Array[String]): Unit = {
        val newArgs = parseArgs(args)
        println(s"host: ${host}, port: ${port}")

        def connectServer(reTryNum: Int): Socket = {
            if (reTryNum < 0) {
                println(s"connect error $reTrySum times.exit")
                System.exit(-1)
            }

            try {
                new Socket(host, port)
            } catch {
                case e: Throwable => e.printStackTrace()
                    println(s"re try $reTryNum")
                    Thread.sleep(1000)
                    connectServer(reTryNum - 1)
            }
        }

        val s = connectServer(reTrySum)

        //构建IO
        val is = s.getInputStream
        val os = s.getOutputStream

        //读取服务器返回的消息
        val br = new BufferedReader(new InputStreamReader(is))
        val mess: String = br.readLine
        System.out.println("server：" + mess)
        println("args: " + newArgs.toList)
        println("start su spark shell ... ")

        val in = new BufferedReader(new InputStreamReader(is))
        val out = new PrintWriter(os, true)

        System.setIn(is)
        Console.setOut(new PrintStream(os))
        Console.setErr(new PrintStream(os))
        System.setOut(new PrintStream(os))
        System.setErr(new PrintStream(os))

        System.setProperty("scala.usejavacp", "true")

        /** [[scala.tools.nsc.interpreter.ReplProps.promptString]] */
        Prop[String]("scala.repl.prompt").set(SPARK_REPL_IGNORE_PROMPT)

        /** [[scala.tools.nsc.interpreter.ReplProps.continueString]] */
        Prop[String]("scala.repl.continue").set("")

        org.apache.spark.repl.Main.conf.set(UI_SHOW_CONSOLE_PROGRESS, true)

        org.apache.spark.repl.Main
            .doMain(newArgs,
                new SuSparkILoop(in, out)
            )
    }
}

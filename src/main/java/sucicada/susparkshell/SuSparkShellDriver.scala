package sucicada.susparkshell

import org.apache.commons.lang3.StringUtils
import sucicada.susparkshell.FlagConstant.{SPARK_REPL_IGNORE_PROMPT, SPARK_REPL_NEW_PROMPT, SU_REPL_PROMPT, SU_SPARK_REPL_INIT_OVER}

import java.io._
import java.net.{InetAddress, ServerSocket}
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.tools.nsc.interpreter.SuReal

object SuSparkShellDriver {
    var running = false
    var inputEnable = false
    var inputOver = false // 输入结束, 等待远端结果
    var getRemoteMsgThread: Thread = _
    var sendLocalInputThread: Thread = _
    var port: Int = -1
    var sparkSubmitMode: String = _


    def getIP() = {
        InetAddress.getLocalHost.getHostAddress
    }

    def parseArgs(args: Array[String]) = {
        var i = 0
        val newArgs = ArrayBuffer[String]()
        while (i < args.length) {
            args(i) match {
                case s if s equals "--su.driver.port" =>
                    port = args(i + 1).toInt
                    newArgs += s += args(i + 1)
                    i += 1
                case "--su.spark-submit.mode" =>
                    sparkSubmitMode = args(i + 1)
                    i += 1
                case s =>
                    newArgs += s
            }
            i += 1
        }
        newArgs += "--su.driver.host" += getIP()
        newArgs.toArray
    }

    //    def getInput() = {
    //        val sets = new Settings
    //        if (sets.classpath.isDefault) {
    //            sets.classpath.value = sys.props("java.class.path")
    //        }
    //    }

    def main(args: Array[String]): Unit = {
        val newArgs = parseArgs(args)

        val serverSocket = new ServerSocket(port)
        try {
            System.out.println(s"启动服务器.... in: ${port}")

            val sparkRealThread = new SparkRealThread(newArgs, sparkSubmitMode)
            sparkRealThread.start()

            val socket = serverSocket.accept
            System.out.println("客户端:" + socket.getInetAddress.getHostAddress + "已连接到服务器")
            //读取客户端发送来的消息
            val sbr = new BufferedReader(new InputStreamReader(socket.getInputStream))
            val sbw = new PrintWriter(new OutputStreamWriter(socket.getOutputStream), true)
            sbw.println("connect success")

            // 从控制台读取输入数据
            //            val in = Console.in
            // 将从 socket 读取的数据输出到屏幕
            //            val out = new PrintWriter(Console.out, true)

            running = true
            val suReal = new SuReal(newArgs, (str) => {
                sbw.println(str)
                inputEnable = false
                inputOver = true
                while (!inputEnable) {
                    Thread.sleep(200)
                }
                // 等待锁结束, 开始新的一轮输入
                inputOver = false
            })
            // 读取远端的输入, 并输出到本地屏幕
            val ignoreReplPattern = s"$SPARK_REPL_IGNORE_PROMPT(.*)".r

            getRemoteMsgThread = new Thread() {
                override def run(): Unit = {
                    println("output started. wait init.....")
                    while (running) {
                        try {
                            //                            val s = interruptableReadLine(sbr)
                            val str = sbr.readLine
                            //                            println(s)
                            str match {
                                case null =>
                                case s if s.startsWith("[Stage") => System.err.print('\r' + s)
                                case SU_SPARK_REPL_INIT_OVER => sendLocalInputThread.start()
                                case SPARK_REPL_NEW_PROMPT => inputEnable = true
                                case ignoreReplPattern(s) => if (StringUtils.isNotEmpty(s)) println(s)
                                case s: String => println(s)
                                //                                case s if StringUtils.isNotBlank(s) => println(s)
                                //                                case _ =>
                            }
                        } catch {
                            case e: Throwable => {
                                e.printStackTrace()
                                running = false
                                socket.close()
                                println("socket close ... , click any to restart")
                            }
                        }
                    }
                    println("output over")
                }
            }

            // 读取本地控制台输入, 并输出到远端
            sendLocalInputThread = new Thread() {
                override def run(): Unit = {
                    println("input started")

                    suReal.start()
                    //                                        while (running) {
                    //                                            val s = interruptableReadLine(in)
                    //                                            sbw.println(s)
                    //                                        }
                }
            }
            getRemoteMsgThread.start()
            //            sendLocalInputThread.start()

            getRemoteMsgThread.join()
            sendLocalInputThread.join()
            sparkRealThread.join()

            serverSocket.close()
            main(args)
        } catch {
            case _: Throwable =>
                serverSocket.close()
                main(args)
        }
    }

    /**
     * https://stackoverflow.com/a/42074828
     */
    @throws[InterruptedException]
    @throws[IOException]
    def interruptableReadLine(reader: BufferedReader): String = {
        var interrupted = false
        val result = new StringBuilder
        var newLine = false
        var chr: Int = -1
        do {
            while (running && !reader.ready) {
                //                if (!inputOver) inputEnable = true
                Thread.sleep(200)
            }
            if (running && reader.ready) {
                chr = reader.read
                if (chr.toChar == '\n' || chr.toChar == '\r') newLine = true
                else if (chr > -1) result.append(chr.toChar)
                interrupted = Thread.interrupted // resets flag, call only once
            }
        } while ( {
            running && !interrupted && !newLine
        })
        if (interrupted) throw new InterruptedException
        if (newLine) result.toString()
        else null
    }
}

class SparkRealThread(args: Array[String], sparkSubmitMode: String) extends Thread {
    val mainClass = "org.apache.spark.repl.SuSparkRepl"
    val jarPath = new File(classOf[org.apache.spark.repl.SuSparkRepl].getProtectionDomain.getCodeSource.getLocation.toURI).getPath
    val (sparkArgs, suArgs) = {
        val sparkArgs, suArgs = ArrayBuffer[String]()
        var i = 0
        while (i < args.length) {
            val s = args(i);
            if (s.startsWith("--su.")) {
                suArgs += s += args(i + 1)
                i += 1
            } else sparkArgs += s
            i += 1
        }
        (sparkArgs.toArray, suArgs.toArray)
    }

    def classMode(): Unit = {
        org.apache.spark.deploy.SparkSubmit.main((ArrayBuffer(
            "--class", mainClass)
            ++= sparkArgs
            += jarPath
            ++= suArgs
            ).toArray
        )
    }

    def shellMode(): Unit = {
        val cmd =
            s"""
               |spark-submit
               | --class $mainClass
               | ${sparkArgs.mkString(" ")}
               | $jarPath
               | ${suArgs.mkString(" ")}
               |"""
                .stripMargin
                .replace("\n", " ")
                .split("[\r\n ]+")
                .filter(_.nonEmpty)
        println("cmd: " + cmd.toList)
        import scala.collection.JavaConverters._
        //        val aa: java.util.List[String] = cmd.toList.asJava
        val pb = new ProcessBuilder(cmd.toList.asJava)
        pb
            .redirectErrorStream(true)
            .redirectOutput(ProcessBuilder.Redirect.PIPE)
            .redirectError(ProcessBuilder.Redirect.PIPE)
        //        pb.inheritIO()
        val process = pb.start()
        //        val process = Runtime.getRuntime.exec(cmd)
        val in = new SequenceInputStream(process.getInputStream, process.getErrorStream)
        //        val inReader = new BufferedReader(new InputStreamReader(in))
        val input = Source.fromInputStream(in).getLines()
        //        System.setOut(System.out)
        //        System.setErr(System.out)
        //        scala.Console.setOut(System.out)
        //        scala.Console.setErr(System.out)
        //        println("nihoa")
        //        while(true){
        //            System.out.println(inReader.readLine())
        //        }
        while (input.hasNext) {
            val s = input.next()
            if (!s.matches(".*INFO yarn.Client: Application report for application.* \\(state: RUNNING\\)")) {
                println(s)
            }
        }
    }

    override def run(): Unit = {
        println("sparkSubmitMode " + sparkSubmitMode)
        println("jarPath " + jarPath)
        println("args: " + args.toList)
        println("sparkArgs: " + sparkArgs.toList)
        println("suArgs: " + suArgs.toList)
        sparkSubmitMode match {
            case "CLASS" => classMode()
            case "SHELL" => shellMode()
            case _ =>
                println("need option: --su.spark-submit.mode  CLASS/SHELL")
                System.exit(-1)
        }
    }
}

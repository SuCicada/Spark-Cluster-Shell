package org.apache.spark.repl

import sucicada.susparkshell.FlagConstant.{SPARK_REPL_NEW_PROMPT, SU_SPARK_REPL_INIT_OVER}

import java.io.BufferedReader
import scala.annotation.tailrec
import scala.concurrent.Future
import scala.reflect.classTag
import scala.reflect.io.File
import scala.tools.nsc.interpreter.StdReplTags.tagOfIMain
import scala.tools.nsc.{GenericRunnerSettings, Properties, Settings}
import scala.tools.nsc.interpreter.{AbstractOrMissingHandler, ILoop, IMain, InteractiveReader, JPrintWriter, NamedParam, ReplProps, SimpleReader, SplashLoop, SplashReader, isReplDebug, isReplPower, replProps}
import scala.util.Properties.{javaVersion, javaVmName, versionString}

class SuSparkILoop(in0: Option[BufferedReader], out: JPrintWriter)
    extends SparkILoop(in0, out) {
    def this(in0: BufferedReader, out: JPrintWriter) = this(Some(in0), out)

    def this() = this(None, new JPrintWriter(Console.out, true))

    val myInitializationCommands = initializationCommands ++ Seq(
        """
            println("myInitializationCommands set over")
        """)


    def initMyProp() = {
        val field = classOf[SparkILoop].getDeclaredField("initializationCommands")
        field.setAccessible(true)
        field.set(this, myInitializationCommands)

        classOf[ReplProps].getDeclaredField("continueText") match {
            case field => field.setAccessible(true)
                field.set(replProps, "")
        }
        println(s"after replProps.continueText:${replProps.continueText}.")

        classOf[ReplProps].getDeclaredField("continuePrompt") match {
            case field => field.setAccessible(true)
                field.set(replProps, "")
        }
        println(s"after replProps.continuePrompt:${replProps.continuePrompt}.")
    }

    override def initializeSpark(): Unit = {
        super.initializeSpark()
    }

    override def printWelcome() {
        import org.apache.spark.SPARK_VERSION
        val suspark_shell_version = "socket-cluster v1.0"
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
        echo(SU_SPARK_REPL_INIT_OVER)
    }

    /** Reader to use before interpreter is online. */
    def preLoop(newReader: InteractiveReader) = {
        val sr = SplashReader(newReader) { r =>
            in = r
            in.postInit()
        }
        in = sr
        SplashLoop(sr, prompt)
    }

    override def process(settings: Settings): Boolean = superRunClosure {
        initMyProp()

        def newReader = in0.fold(chooseReader(settings))(r => SimpleReader(r, out, interactive = true))

        /* Actions to cram in parallel while collecting first user input at prompt.
         * Run with output muted both from ILoop and from the intp reporter.
         */
        def loopPostInit(): Unit = mumly {
            // Bind intp somewhere out of the regular namespace where
            // we can get at it in generated code.
            intp.quietBind(NamedParam[IMain]("$intp", intp)(tagOfIMain, classTag[IMain]))

            // Auto-run code via some setting.
            (replProps.replAutorunCode.option
                flatMap (f => File(f).safeSlurp())
                foreach (intp quietRun _)
                )
            // power mode setup
            if (isReplPower) enablePowerMode(true)
            initializeSpark()
            loadInitFiles()
            // SI-7418 Now, and only now, can we enable TAB completion.
            in.postInit()
        }

        def loadInitFiles(): Unit = settings match {
            case settings: GenericRunnerSettings =>
                for (f <- settings.loadfiles.value) {
                    loadCommand(f)
                    addReplay(s":load $f")
                }
                for (f <- settings.pastefiles.value) {
                    pasteCommand(f)
                    addReplay(s":paste $f")
                }
            case _ =>
        }

        // wait until after startup to enable noisy settings
        def withSuppressedSettings[A](body: => A): A = {
            val ss = this.settings
            import ss._
            val noisy = List(Xprint, Ytyperdebug)
            val noisesome = noisy.exists(!_.isDefault)
            val current = (Xprint.value, Ytyperdebug.value)
            if (isReplDebug || !noisesome) body
            else {
                this.settings.Xprint.value = List.empty
                this.settings.Ytyperdebug.value = false
                try body
                finally {
                    Xprint.value = current._1
                    Ytyperdebug.value = current._2
                    intp.global.printTypings = current._2
                }
            }
        }

        def startup(): String = withSuppressedSettings {
            // let them start typing
            val splash = preLoop(newReader)

            // while we go fire up the REPL
            try {
                // don't allow ancient sbt to hijack the reader
                savingReader {
                    createInterpreter()
                }
                intp.initializeSynchronous()

                val field = classOf[ILoop].getDeclaredFields.filter(_.getName.contains("globalFuture")).head
                field.setAccessible(true)
                field.set(this, Future successful true)

                if (intp.reporter.hasErrors) {
                    echo("Interpreter encountered errors during initialization!")
                    null
                } else {
                    loopPostInit()
                    printWelcome()
                    splash.start()

                    val line = splash.line // what they typed in while they were waiting
                    if (line == null) { // they ^D
                        try out print Properties.shellInterruptedString
                        finally closeInterpreter()
                    }
                    line
                }
            } finally splash.stop()
        }

        this.settings = settings
        startup() match {
            case null => false
            case line =>
                try myLoop(line) match {
                    case LineResults.EOF => out print Properties.shellInterruptedString
                    case _ =>
                }
                catch AbstractOrMissingHandler()
                finally closeInterpreter()
                true
        }
    }

    def superRunClosure(body: => java.lang.Boolean): Boolean = {
        val method = classOf[SparkILoop].getDeclaredMethod(
            "runClosure", classOf[() => Boolean])
        method.setAccessible(true)
        method.invoke(this, body).asInstanceOf[Boolean]
    }

    import LineResults.LineResult

    def myLoop(line: String): LineResult = {
        import LineResults._
        if (line == null) EOF
        else if (try processLine(line) catch crashRecovery) myLoop(readOneLine())
        else ERR
    }

    val crashRecovery = {
        val field = classOf[scala.tools.nsc.interpreter.ILoop].getDeclaredField("crashRecovery")
        field.setAccessible(true)
        field.get(this).asInstanceOf[PartialFunction[Throwable, Boolean]]
    }

    private def readOneLine() = {
        out.flush()
        out.println(SPARK_REPL_NEW_PROMPT)
        out.flush()
        in readLine ""
    }
}

package scala.tools.nsc.interpreter

import java.io.PrintWriter
import scala.sys.Prop
import scala.tools.nsc.MainGenericRunner.errorFn
import scala.tools.nsc.interpreter.Results.{Incomplete, Success}
import scala.tools.nsc.{GenericRunnerCommand, Settings}

class SuReal(args: Array[String], inputAction: (String) => Unit) {
    var settings: Settings = _

    def start(): Unit = {
        System.setProperty("scala.usejavacp", "true")
        val command = new GenericRunnerCommand(args.toList, (x: String) => errorFn(x))

        settings = command.settings

        //        /** [[scala.tools.nsc.interpreter.ReplProps.continueString]] */
        //        Prop[String]("scala.repl.continue").set("")

        new SuILoop process settings

        /** [[ scala.tools.nsc.MainGenericRunner.main(Array()) ]] */
    }

    def sendCommand(cmd: String) = {
        inputAction(cmd)
//        printWriter.println(cmd)
    }

    /**
     * 重点在 [[scala.tools.nsc.interpreter.ILoop.interpretStartingWith]]
     */
    class SuILoop extends ILoop {
        override def createInterpreter() {
            if (addedClasspath != "")
                settings.classpath append addedClasspath
            intp = new SuILoopInterpreter
        }

        //        override def command(line: String): Result = {
        //            if (line startsWith ":") super.command(line.tail)
        //            else if (intp.global == null) Result(keepRunning = false, None) // Notice failure to create compiler
        //            else {
        //                val res = interpretStartingWith(line)
        //                println("res  " + res)
        //                Result(keepRunning = true, res)
        //            }
        //        }

        class SuILoopInterpreter extends IMain(settings, out) {
            override protected def parentClassLoader =
                settings.explicitParentLoader.getOrElse(classOf[ILoop].getClassLoader)

            val checkInterpreter = new ILoopInterpreter

            /**
             * 579
             * [[scala.tools.nsc.interpreter.IMain.compile(line: String, synthetic: Boolean): Either[IR.Result, Request]]]
             */
            def compile(line: String, synthetic: Boolean): Either[IR.Result, Request] = {
                //                val method = classOf[scala.tools.nsc.interpreter.IMain].getDeclaredMethod(
                //                    "compile", classOf[String], classOf[Boolean])
                //                method.setAccessible(true)
                //                method.invoke(this, line, Boolean.box(synthetic)).asInstanceOf[Either[IR.Result, Request]]

                if (global == null) Left(IR.Error)
                else requestFromLine(line, synthetic) match {
                    case Left(result) => Left(result)
                    case Right(req) =>
                        // null indicates a disallowed statement type; otherwise compile and
                        // fail if false (implying e.g. a type error)
                        //                        if (req == null || !req.compile) Left(IR.Error) else
                        Right(req)
                }
            }

            override def interpret(line: String, synthetic: Boolean): IR.Result = {
                if (!synthetic) {
                    //                    println(line)
                    val res = compile(line, synthetic)
                    //                    println(res)
                    //                    println("input: " + line)
                    res match {
                        case Left(Incomplete) => Incomplete
                        case _ => {
                            //                            println(line)
                            sendCommand(line)
                            Success
                        }
                    }
                } else {
                    Success
                }
            }
        }
    }
}

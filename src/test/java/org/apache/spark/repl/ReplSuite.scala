package org.apache.spark.repl

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION

import java.io._
import java.net.URLClassLoader
import scala.collection.mutable.ArrayBuffer
import scala.tools.nsc.interpreter.SimpleReader

object ReplSuite {


    def main(args: Array[String]): Unit = {

        val c = new Configuration()
        val fs = FileSystem.get(c)
        println(fs.listStatus(new Path("/")))


        val output = runInterpreter("localquiet",
            """
             def myMethod() = "first definition"
              |val tmp = myMethod(); val out = tmp
              |def myMethod() = "second definition"
              |val tmp = myMethod(); val out = s"$tmp aabbcc"
        """.stripMargin)
        println(output)
    }

    def runInterpreter(master: String, input: String): String = {
        val CONF_EXECUTOR_CLASSPATH = "spark.executor.extraClassPath"

        val in = new BufferedReader(new StringReader(input + "\n"))
        val out = new StringWriter()
        val cl = getClass.getClassLoader
        var paths = new ArrayBuffer[String]
        if (cl.isInstanceOf[URLClassLoader]) {
            val urlLoader = cl.asInstanceOf[URLClassLoader]
            for (url <- urlLoader.getURLs) {
                if (url.getProtocol == "file") {
                    paths += url.getFile
                }
            }
        }
        val classpath = paths.map(new File(_).getAbsolutePath).mkString(File.pathSeparator)
        // scalastyle:off println
        println(classpath)
        val oldExecutorClasspath = System.getProperty(CONF_EXECUTOR_CLASSPATH)
        System.setProperty(CONF_EXECUTOR_CLASSPATH, classpath)
        Main.sparkContext = null
        Main.sparkSession = null // causes recreation of SparkContext for each test.
        Main.conf.set("spark.master", master)
        Main.doMain(Array("-classpath", classpath),
            new SparkILoop(in, new PrintWriter(out)))

        if (oldExecutorClasspath != null) {
            System.setProperty(CONF_EXECUTOR_CLASSPATH, oldExecutorClasspath)
        } else {
            System.clearProperty(CONF_EXECUTOR_CLASSPATH)
        }
        return out.toString
    }

    // Simulate the paste mode in Scala REPL.
    def runInterpreterInPasteMode(master: String, input: String): String =
        runInterpreter(master, ":paste\n" + input + 4.toChar) // 4 is the ascii code of CTRL + D

    def assertContains(message: String, output: String) {
        val isContain = output.contains(message)
        assert(isContain,
            "Interpreter output did not contain '" + message + "':\n" + output)
    }

    def assertDoesNotContain(message: String, output: String) {
        val isContain = output.contains(message)
        assert(!isContain,
            "Interpreter output contained '" + message + "':\n" + output)
    }
}

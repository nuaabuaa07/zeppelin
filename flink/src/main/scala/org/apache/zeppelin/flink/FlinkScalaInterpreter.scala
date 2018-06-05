/**
  * Licensed to the Apache Software Foundation (ASF) under one
  * or more contributor license agreements.  See the NOTICE file
  * distributed with this work for additional information
  * regarding copyright ownership.  The ASF licenses this file
  * to you under the Apache License, Version 2.0 (the
  * "License"); you may not use this file except in compliance
  * with the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package org.apache.zeppelin.flink

import java.io.{BufferedReader, File}
import java.util.Properties

import org.apache.flink.api.scala.FlinkShell._
import org.apache.flink.api.scala.{ExecutionEnvironment, FlinkILoop}
import org.apache.flink.configuration.GlobalConfiguration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala.{BatchTableEnvironment, StreamTableEnvironment}
import org.apache.zeppelin.interpreter.thrift.InterpreterCompletion
import org.apache.zeppelin.interpreter.util.InterpreterOutputStream
import org.apache.zeppelin.interpreter.{InterpreterContext, InterpreterResult}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.Completion.ScalaCompleter
import scala.tools.nsc.interpreter.{JPrintWriter, SimpleReader}

class FlinkScalaInterpreter(val properties: Properties) {

  lazy val LOGGER: Logger = LoggerFactory.getLogger(getClass)

  private var flinkILoop: FlinkILoop = _
  private var scalaCompleter: ScalaCompleter = _
  private val interpreterOutput = new InterpreterOutputStream(LOGGER)

  private var benv: ExecutionEnvironment = _
  private var senv: StreamExecutionEnvironment = _
  private var btenv: BatchTableEnvironment = _
  private var stenv: StreamTableEnvironment = _

  def open(): Unit = {
    var config = Config(executionMode =
      ExecutionMode.withName(properties.getProperty("flink.execution.mode", "LOCAL").toUpperCase))
    val containerNum = Integer.parseInt(properties.getProperty("flink.yarn.num_container", "1"))
    config = config.copy(yarnConfig =
      Some(ensureYarnConfig(config).copy(containers = Some(containerNum))))

    val confDirPath = "/Users/jzhang/github/zeppelin/flink/src/test/resources"
    val configDirectory = new File(confDirPath)
    val configuration = GlobalConfiguration.loadConfiguration(configDirectory.getAbsolutePath)

    val replOut = new JPrintWriter(interpreterOutput, true)
    val (iLoop, cluster) = try {
      val (host, port, cluster) = fetchConnectionInfo(config)
      val conf = cluster match {
        case Some(Left(miniCluster)) => miniCluster.getConfiguration
        case Some(Right(yarnCluster)) => yarnCluster.getFlinkConfiguration
        case None => configuration
      }

      println(s"\nConnecting to Flink cluster (host: $host, port: $port).\n")
      val repl = new FlinkILoop(host, port, conf, config.externalJars, None, replOut)
      (repl, cluster)
    } catch {
      case e: IllegalArgumentException =>
        println(s"Error: ${e.getMessage}")
        sys.exit()
    }

    this.flinkILoop = iLoop
    val settings = new Settings()
    settings.usejavacp.value = true
    settings.Yreplsync.value = true
    flinkILoop.settings = settings
    flinkILoop.createInterpreter()

    val in0 = getField(flinkILoop, "scala$tools$nsc$interpreter$ILoop$$in0").asInstanceOf[Option[BufferedReader]]
    val reader = in0.fold(flinkILoop.chooseReader(settings))(r => SimpleReader(r, replOut, interactive = true))

    flinkILoop.in = reader
    flinkILoop.initializeSynchronous()
    callMethod(flinkILoop, "scala$tools$nsc$interpreter$ILoop$$loopPostInit")
    this.scalaCompleter = reader.completion.completer()

    this.benv = flinkILoop.scalaBenv
    this.senv = flinkILoop.scalaSenv
    this.btenv = TableEnvironment.getTableEnvironment(this.benv).asInstanceOf[BatchTableEnvironment]
    this.stenv = TableEnvironment.getTableEnvironment(this.senv).asInstanceOf[StreamTableEnvironment]
    bind("btenv", btenv.getClass.getCanonicalName, btenv, List("@transient"))
    bind("stenv", stenv.getClass.getCanonicalName, stenv, List("@transient"))

    //    senv.getConfig.disableSysoutLogging
    //    benv.getConfig.disableSysoutLogging
  }

  // for use in java side
  protected def bind(name: String,
                     tpe: String,
                     value: Object,
                     modifier: java.util.List[String]): Unit = {
    flinkILoop.beQuietDuring {
      flinkILoop.bind(name, tpe, value, modifier.asScala.toList)
    }
  }

  protected def bind(name: String,
                     tpe: String,
                     value: Object,
                     modifier: List[String]): Unit = {
    flinkILoop.beQuietDuring {
      flinkILoop.bind(name, tpe, value, modifier)
    }
  }

  protected def completion(buf: String,
                           cursor: Int,
                           context: InterpreterContext): java.util.List[InterpreterCompletion] = {
    val completions = scalaCompleter.complete(buf, cursor).candidates
      .map(e => new InterpreterCompletion(e, e, null))
    scala.collection.JavaConversions.seqAsJavaList(completions)
  }

  protected def callMethod(obj: Object, name: String): Object = {
    callMethod(obj, name, Array.empty[Class[_]], Array.empty[Object])
  }

  protected def callMethod(obj: Object, name: String,
                           parameterTypes: Array[Class[_]],
                           parameters: Array[Object]): Object = {
    val method = obj.getClass.getMethod(name, parameterTypes: _ *)
    method.setAccessible(true)
    method.invoke(obj, parameters: _ *)
  }


  protected def getField(obj: Object, name: String): Object = {
    val field = obj.getClass.getField(name)
    field.setAccessible(true)
    field.get(obj)
  }

  def interpret(code: String, context: InterpreterContext): InterpreterResult = {
    if (context != null) {
      interpreterOutput.setInterpreterOutput(context.out)
      context.out.clear()
    }

    Console.withOut(if (context != null) context.out else Console.out) {
      // redirect java stdout as well, because Dataset.print use the java stdout
      val originalOut = System.out
      System.setOut(Console.out)
      interpreterOutput.ignoreLeadingNewLinesFromScalaReporter()
      // add print("") at the end in case the last line is comment which lead to INCOMPLETE
      val lines = code.split("\\n") ++ List("print(\"\")")
      var incompleteCode = ""
      var lastStatus: InterpreterResult.Code = null
      for (line <- lines if !line.trim.isEmpty) {
        val nextLine = if (incompleteCode != "") {
          incompleteCode + "\n" + line
        } else {
          line
        }
        flinkILoop.interpret(nextLine) match {
          case scala.tools.nsc.interpreter.IR.Success =>
            // continue the next line
            incompleteCode = ""
            lastStatus = InterpreterResult.Code.SUCCESS
          case error@scala.tools.nsc.interpreter.IR.Error =>
            return new InterpreterResult(InterpreterResult.Code.ERROR)
          case scala.tools.nsc.interpreter.IR.Incomplete =>
            // put this line into inCompleteCode for the next execution.
            incompleteCode = incompleteCode + "\n" + line
            lastStatus = InterpreterResult.Code.INCOMPLETE
        }
      }
      // flush all output before returning result to frontend
      Console.flush()
      interpreterOutput.setInterpreterOutput(null)
      // reset the java stdout
      System.setOut(originalOut)
      return new InterpreterResult(lastStatus)
    }
  }

  def close(): Unit = {
    if (flinkILoop != null) {
      flinkILoop.close()
    }
  }

  def getExecutionEnviroment(): ExecutionEnvironment = this.benv

  def getStreamingExecutionEnviroment(): StreamExecutionEnvironment = this.senv

  def getBatchTableEnviroment(): BatchTableEnvironment = this.btenv

//  def getStreamTableEnviroment(): TableEnvironment = this.stenv
}

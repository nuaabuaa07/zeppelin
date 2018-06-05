package org.apache.zeppelin.flink

import org.apache.flink.api.scala.DataSet
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.types.Row
import org.apache.zeppelin.interpreter.{InterpreterContext, InterpreterResult}
import org.apache.flink.table.api.scala._
import org.apache.flink.streaming.api.scala._

class FlinkSQLScalaInterpreter(scalaInterpreter: FlinkScalaInterpreter) {

  private var btenv: BatchTableEnvironment = scalaInterpreter.getBatchTableEnviroment()


  def interpret(code: String, context: InterpreterContext): InterpreterResult = {
    val table: Table = this.btenv.sql(code)
    val dsRow: DataSet[Row] = btenv.toDataSet[Row](table)
    try {
      val builder: StringBuilder = new StringBuilder
      val columnNames: Array[String] = table.getSchema.getColumnNames
      builder.append(columnNames.mkString("\t"))
      builder.append("\n")

      val rows = dsRow.first(1000).collect()
      for (row <- rows) {
        var i = 0;
        while (i < row.getArity) {
          builder.append(row.getField(i))
          i += 1
          if (i != row.getArity) {
            builder.append("\t");
          }
        }
        builder.append("\n")
      }
      return new InterpreterResult(
        InterpreterResult.Code.SUCCESS, InterpreterResult.Type.TABLE, builder.toString)
    } catch {
      case e: Exception =>
        return new InterpreterResult(InterpreterResult.Code.ERROR,
          "Fail to fetch result: " + e.getMessage)
    }
  }
}

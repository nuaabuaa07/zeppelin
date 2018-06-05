/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zeppelin.flink;


import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterException;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.LazyOpenInterpreter;
import org.apache.zeppelin.interpreter.WrappedInterpreter;

import java.util.Properties;

public class FlinkSQLInterpreter extends Interpreter {

  private FlinkSQLScalaInterpreter sqlScalaInterpreter;

  public FlinkSQLInterpreter(Properties properties) {
    super(properties);
  }

  @Override
  public void open() throws InterpreterException {
    FlinkInterpreter flinkInterpreter = getFlinkInterpreter();
    this.sqlScalaInterpreter = new FlinkSQLScalaInterpreter(
        flinkInterpreter.getInnerScalaInterpreter());
  }

  private FlinkInterpreter getFlinkInterpreter() throws InterpreterException {
    LazyOpenInterpreter lazy = null;
    FlinkInterpreter flink = null;
    Interpreter p = getInterpreterInTheSameSessionByClassName(FlinkInterpreter.class.getName());

    while (p instanceof WrappedInterpreter) {
      if (p instanceof LazyOpenInterpreter) {
        lazy = (LazyOpenInterpreter) p;
      }
      p = ((WrappedInterpreter) p).getInnerInterpreter();
    }
    flink = (FlinkInterpreter) p;

    if (lazy != null) {
      lazy.open();
    }
    return flink;
  }

  @Override
  public void close() throws InterpreterException {

  }

  @Override
  public InterpreterResult interpret(String st, InterpreterContext context)
      throws InterpreterException {
    return sqlScalaInterpreter.interpret(st, context);
  }

  @Override
  public void cancel(InterpreterContext context) throws InterpreterException {

  }

  @Override
  public FormType getFormType() throws InterpreterException {
    return FormType.SIMPLE;
  }

  @Override
  public int getProgress(InterpreterContext context) throws InterpreterException {
    return 0;
  }
}

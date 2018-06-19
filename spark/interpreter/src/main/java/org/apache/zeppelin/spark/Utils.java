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

package org.apache.zeppelin.spark;

import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.user.AuthenticationInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility and helper functions for the Spark Interpreter
 */
class Utils {
  public static Logger logger = LoggerFactory.getLogger(Utils.class);

  public static String buildJobGroupId(InterpreterContext context) {
    return "zeppelin-" + context.getNoteId() + "-" + context.getParagraphId();
  }

  public static String buildJobDesc(InterpreterContext context) {
    return "Started by: " + getUserName(context.getAuthenticationInfo());
  }

  public static String getNoteId(String jobgroupId) {
    int indexOf = jobgroupId.indexOf("-");
    int secondIndex = jobgroupId.indexOf("-", indexOf + 1);
    return jobgroupId.substring(indexOf + 1, secondIndex);
  }

  public static String getParagraphId(String jobgroupId) {
    int indexOf = jobgroupId.indexOf("-");
    int secondIndex = jobgroupId.indexOf("-", indexOf + 1);
    return jobgroupId.substring(secondIndex + 1, jobgroupId.length());
  }

  public static String getUserName(AuthenticationInfo info) {
    String uName = "";
    if (info != null) {
      uName = info.getUser();
    }
    if (uName == null || uName.isEmpty()) {
      uName = "anonymous";
    }
    return uName;
  }
}

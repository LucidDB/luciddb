/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/

package org.eigenbase.applib.script;

import java.sql.*;

/**
 * Simple function to create other applib scripting functions that
 * explicitly take different numbers of arguments one can then pass to a script
 * as a list.
 *
 * Currently does not work.
 *
 * @author Kevin Secretan
 * @version $Id$
 */
public abstract class MakeScriptFuncUdp
{

  /**
   * Creates an applib function making use of the generic function defined
   * in the Udr file.
   */
  public static void execute(
      String funcName,
      String inputNames)
    throws SQLException
  {
    Connection conn = DriverManager.getConnection("jdbc:default:connection");

    StringBuffer sb = new StringBuffer();
    PreparedStatement ps = conn.prepareStatement("set schema 'localdb.applib'");
    ps.execute();
    sb.append("create or replace function " + funcName + "(");
    sb.append(
        "\n  engine_name varchar(255),"
        + "\n  script varchar(65535),"
        + "\n  func_name varchar(255),");
    for (String name : inputNames.split(",")) {
      sb.append("\n  " + name + " varchar(65535),");
    }
    sb.setCharAt(sb.length()-1, ')');
    // How do get LucidDB to call the right method?
    // The right class arg of "[Ljava.lang.String;" does not make it work.
    sb.append(
        "\nreturns varchar(65535)\n"
        + "language java\n"
        + "parameter style system defined java\n"
        + "modifies sql data\n"
        + "external name "
        + "'applib.applibJar:org.eigenbase.applib.script.ExecuteScriptUdr."
        + "executeGeneralUdf(java.lang.String, java.lang.String, "
        + "java.lang.String, java.util.List)'");
    ps = conn.prepareStatement(sb.toString());
    ps.execute();
  }

}

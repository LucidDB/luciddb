/*
// $Id$
// Applib is a library of SQL-invocable routines for Eigenbase applications.
// Copyright (C) 2011 The Eigenbase Project
// Copyright (C) 2011 SQLstream, Inc.
// Copyright (C) 2011 DynamoBI Corporation
//
// This library is free software; you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation; either version 2.1 of the License, or (at
// your option) any later version.
//
// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public
// License along with this library; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
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

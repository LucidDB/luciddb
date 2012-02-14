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
package net.sf.farrago.syslib;

import java.sql.*;

import java.util.logging.*;

import net.sf.farrago.db.*;
import net.sf.farrago.jdbc.*;
import net.sf.farrago.runtime.*;
import net.sf.farrago.session.*;
import net.sf.farrago.trace.*;


/**
 * FarragoKillUDR defines some system procedures for killing sessions and
 * executing statements. (Technically these are user-defined procedures.) They
 * are intended for use by a system administrator, and are installed by
 * initsql/createMgmtViews.sql
 *
 * @author Marc Berkowitz
 * @version $Id$
 */
public abstract class FarragoKillUDR
{
    //~ Static fields/initializers ---------------------------------------------

    static Logger tracer = FarragoTrace.getSyslibTracer();

    //~ Methods ----------------------------------------------------------------

    /**
     * Kills a running session, destroying it and releasing any resources
     * associated with it.
     *
     * @param id unique session identifier
     */
    public static void killSession(long id)
        throws SQLException
    {
        killSession(id, false);
    }

    /**
     * Kills a running session.
     *
     * @param id unique session identifier
     * @param cancelOnly if true, just cancel current execution; if false,
     * destroy session
     */
    public static void killSession(long id, boolean cancelOnly)
        throws SQLException
    {
        try {
            FarragoSession sess = FarragoUdrRuntime.getSession();
            FarragoDatabase db = ((FarragoDbSession) sess).getDatabase();
            db.killSession(id, cancelOnly);
        } catch (Throwable e) {
            throw FarragoJdbcUtil.newSqlException(e, tracer);
        }
    }

    /**
     * Kills an executing statement, destroying it and releasing any resources
     * associated with it.
     *
     * @param id unique statement identifier
     */
    public static void killStatement(long id)
        throws SQLException
    {
        killStatement(id, false);
    }

    /**
     * Kills an executing statement.
     *
     * @param id unique statement identifier
     * @param cancelOnly if true, just cancel current execution; if false,
     * destroy statement
     */
    public static void killStatement(long id, boolean cancelOnly)
        throws SQLException
    {
        try {
            FarragoSession sess = FarragoUdrRuntime.getSession();
            FarragoDatabase db = ((FarragoDbSession) sess).getDatabase();
            db.killExecutingStmt(id, cancelOnly);
        } catch (Throwable e) {
            throw FarragoJdbcUtil.newSqlException(e, tracer);
        }
    }

    /**
     * Kills all statements executing SQL that matches a given substring.
     *
     * @param s substring to look for in statement SQL text
     */
    public static void killStatementMatch(String s)
        throws SQLException
    {
        killStatementMatch(s, false);
    }

    /**
     * Kills all statements executing SQL that matches a given substring.
     *
     * @param s substring to look for in statement SQL text
     * @param cancelOnly if true, just cancel current execution; if false,
     * destroy statement
     */
    public static void killStatementMatch(String s, boolean cancelOnly)
        throws SQLException
    {
        try {
            FarragoSession sess = FarragoUdrRuntime.getSession();
            FarragoDatabase db = ((FarragoDbSession) sess).getDatabase();
            db.killExecutingStmtMatching(
                s,
                "call sys_boot.mgmt.kill_",
                cancelOnly); // exclude self
        } catch (Throwable e) {
            throw FarragoJdbcUtil.newSqlException(e, tracer);
        }
    }
}

// End FarragoKillUDR.java

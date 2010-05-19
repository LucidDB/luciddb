/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 2003 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by the Free
// Software Foundation; either version 2 of the License, or (at your option)
// any later version approved by The Eigenbase Project.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
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

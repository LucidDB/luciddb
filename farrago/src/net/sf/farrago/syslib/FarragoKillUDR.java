/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2006 The Eigenbase Project
// Copyright (C) 2005-2006 Disruptive Tech
// Copyright (C) 2005-2006 LucidEra, Inc.
// Portions Copyright (C) 2003-2006 John V. Sichi
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

import net.sf.farrago.session.FarragoSession;
import net.sf.farrago.db.FarragoDatabase;
import net.sf.farrago.db.FarragoDbSession;
import net.sf.farrago.runtime.FarragoUdrRuntime;
import net.sf.farrago.trace.FarragoTrace;
import net.sf.farrago.jdbc.FarragoJdbcUtil;

import java.sql.SQLException;
import java.util.logging.Logger;

/**
 * FarragoKillUDR defines some system procedures for killing sessions and executing statements.
 * (Technically these are user-defined procedures.)
 * They are intended for use by a system administrator, and are installed by
 * initsql/createMgmtViews.sql
 *
 * @author Marc Berkowitz
 * @version $Id$
 */
public abstract class FarragoKillUDR
{
    static Logger tracer = FarragoTrace.getSyslibTracer();

    /** 
     * Kills a running session
     * @param id unique session identifier
     */
    public static void killSession(long id) throws SQLException
    {
        try {
            FarragoSession sess = FarragoUdrRuntime.getSession();
            FarragoDatabase db = ((FarragoDbSession) sess).getDatabase();
            db.killSession(id);
        } catch (Throwable e) {
            throw FarragoJdbcUtil.newSqlException(e, tracer);
        }
    }

    /** 
     * Kills an executing statement.
     * @param id unique statement identifier
     */
    public static void killStatement(long id) throws SQLException
    {
        try {
            FarragoSession sess = FarragoUdrRuntime.getSession();
            FarragoDatabase db = ((FarragoDbSession) sess).getDatabase();
            db.killExecutingStmt(id);
        } catch (Throwable e) {
            throw FarragoJdbcUtil.newSqlException(e, tracer);
        }
    }

    /**
     * Kills all statements executing SQL that matches a given substring.
     * @param s
     */
    public static void killStatementMatch(String s) throws SQLException
    {
        try {
            FarragoSession sess = FarragoUdrRuntime.getSession();
            FarragoDatabase db = ((FarragoDbSession) sess).getDatabase();
            db.killExecutingStmtMatching(s, "call sys_boot.mgmt.kill_"); // exclude self

        } catch (Throwable e) {
            throw FarragoJdbcUtil.newSqlException(e, tracer);
        }
    }
}

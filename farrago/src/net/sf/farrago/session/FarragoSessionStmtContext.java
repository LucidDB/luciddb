/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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
package net.sf.farrago.session;

import java.sql.*;

import net.sf.farrago.query.FarragoPreparingStmt;
import net.sf.farrago.util.*;

import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.sql.SqlKind;


/**
 * FarragoSessionStmtContext represents a context for executing SQL statements
 * within a particular {@link FarragoSession}.  Contrast with {@link
 * net.sf.farrago.jdbc.engine.FarragoJdbcEngineStatement} (a JDBC wrapper),
 * {@link FarragoSessionPreparingStmt} (which manages the
 * preparation process for a single statement), and {@link
 * FarragoSessionExecutableStmt}, (which is shared by all
 * sessions).
 *
 *<p>
 *
 * TODO:  document statement lifecycle
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface FarragoSessionStmtContext extends FarragoAllocation
{
    //~ Methods ---------------------------------------------------------------

    /**
     * @return the session from which this statement context was created
     */
    public FarragoSession getSession();

    /**
     * @return whether this context currently has a statement prepared
     */
    public boolean isPrepared();

    /**
     * @return whether this context currently has a DML statement prepared
     */
    public boolean isPreparedDml();

    /**
     * Turns this context into a daemon so that it will be deallocated
     * as soon as its current result set is closed.
     */
    public void daemonize();

    /**
     * Prepares an SQL statement.
     *
     * @param sql text of statement to be prepared
     *
     * @param isExecDirect whether the statement is being prepared
     * as part of direct execution
     */
    public void prepare(
        String sql,
        boolean isExecDirect);

    /**
     * Prepares a query or DML statement (not DDL), provided as a query plan.
     * The system uses this to prepare and execute internal statements.
     * As with {@link #prepare(String,boolean)}, the statement can be executed
     * by {@link #execute()}.
     *
     * @param plan a query plan (ie a relational expression).
     * @param kind SqlKind value that characterized the statement.
     * @param logical true when the query plan is logical (needs to be
     *  optimized), false when it is physical (already optimized).
     * @param prep the FarragoSessionPreparingStatement that is managing the
     * query plan.
     */
    public void prepare(
        RelNode plan,
        SqlKind kind,
        boolean logical,
        FarragoSessionPreparingStmt prep);

    /**
     * @return the output row type for the currently prepared statement
     */
    public RelDataType getPreparedRowType();

    /**
     * @return the input parameter row type for the currently prepared
     * statement
     */
    public RelDataType getPreparedParamType();

    /**
     * Sets an input parameter.
     *
     * @param iParam 0-based index of parameter to set
     *
     * @param arg value to set
     */
    public void setDynamicParam(
        int iParam,
        Object arg);

    /**
     * Clears any settings for all dynamic parameters.
     */
    public void clearParameters();

    /**
     * Executes the currently prepared statement.
     */
    public void execute();

    /**
     * @return the result set produced by execute(), or null
     * if the statement was not a query
     */
    public ResultSet getResultSet();

    /**
     * Obtains an update count produced by execute(),
     * clearing this information as a side effect.
     *
     * @return number of rows affected, or -1 if statement is non-DML
     * or its update count was already returned
     */
    public int getUpdateCount();

    /**
     * Closes any result set associated with this statement context.
     */
    public void closeResultSet();

    /**
     * Cancels execution.
     */
    public void cancel();

    /**
     * Releases any resources (including result sets) associated with
     * this statement context.
     */
    public void unprepare();

    public void setQueryTimeout(int milliseconds);

    public int getQueryTimeout();
}


// End FarragoSessionStmtContext.java

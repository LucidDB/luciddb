/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/
package net.sf.farrago.session;

import java.sql.DatabaseMetaData;

import net.sf.farrago.catalog.FarragoRepos;
import net.sf.farrago.util.FarragoAllocation;

import org.eigenbase.oj.rex.OJRexImplementorTable;
import org.eigenbase.sql.SqlOperatorTable;
import org.eigenbase.reltype.*;


/**
 * FarragoSession represents an internal API to the Farrago database.  It is
 * designed to serve as a basis for the implementation of standard API's such
 * as JDBC.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface FarragoSession
    extends FarragoAllocation, FarragoStreamFactoryProvider
{
    //~ Methods ---------------------------------------------------------------

    /**
     * @return table of builtin SQL operators and functions to use for
     * validation; user-defined functions are not included here
     */
    public SqlOperatorTable getSqlOperatorTable();

    /**
     * @return table of implementations corresponding to result
     * of {@link #getSqlOperatorTable}
     */
    public OJRexImplementorTable getOJRexImplementorTable();

    /**
     * @return JDBC URL used to establish this session
     */
    public String getUrl();

    /**
     * @return repos accessed by this session
     */
    public FarragoRepos getRepos();

    /**
     * @return whether this session is an internal session cloned
     * from another session
     */
    public boolean isClone();

    /**
     * @return whether this session has already been closed
     */
    public boolean isClosed();

    /**
     * @return whether this session currently has a transaction in progress
     */
    public boolean isTxnInProgress();

    /**
     * @return whether this session is in autocommit mode
     */
    public boolean isAutoCommit();

    /**
     * @return current connection defaults for this session
     */
    public FarragoSessionVariables getSessionVariables();

    /**
     * @return JDBC database metadata for this session
     */
    public DatabaseMetaData getDatabaseMetaData();

    /**
     * @return connection source
     */
    public FarragoSessionConnectionSource getConnectionSource();

    /**
     * @return name of local data server to use for tables when none
     * is specified by CREATE TABLE
     */
    public String getDefaultLocalDataServerName();

    /**
     * Initializes the database metadata associated with this session.
     *
     * @param dbMetaData metadata to set
     */
    public void setDatabaseMetaData(DatabaseMetaData dbMetaData);

    /**
     * Initializes the connection source associated with this session.
     *
     * @param source connection source to set
     */
    public void setConnectionSource(FarragoSessionConnectionSource source);

    /**
     * Creates a new statement context within this session.
     *
     * @return new statement context
     */
    public FarragoSessionStmtContext newStmtContext();

    /**
     * Creates a new SQL parser.
     *
     * @return new parser
     */
    public FarragoSessionParser newParser();

    /**
     * Creates a new preparing statement tied to this session and its underlying
     * database.  Used to construct and implement an internal query plan.
     *
     * @param stmtValidator generic stmt validator
     *
     * @return a new {@link FarragoSessionPreparingStmt}.
     */
    public FarragoSessionPreparingStmt newPreparingStmt(
        FarragoSessionStmtValidator stmtValidator);

    /**
     * Creates a new SQL statement validator.
     *
     * @return new validator
     */
    public FarragoSessionStmtValidator newStmtValidator();

    /**
     * Creates a new validator for DDL commands.
     *
     * @param stmtValidator generic stmt validator
     *
     * @return new validator
     */
    public FarragoSessionDdlValidator newDdlValidator(
        FarragoSessionStmtValidator stmtValidator);

    /**
     * Creates a new planner.
     *
     * @param stmt stmt on whose behalf planner will operate
     *
     * @param init whether to initialize default rules in new planner
     *
     * @return new planner
     */
    public FarragoSessionPlanner newPlanner(
        FarragoSessionPreparingStmt stmt,
        boolean init);

    /**
     * Clones this session.  TODO:  document what this entails.
     *
     * @param inheritedVariables session variables to use for context
     * in new session, or null to inherit those of session being cloned
     *
     * @return cloned session.
     */
    public FarragoSession cloneSession(
        FarragoSessionVariables inheritedVariables);

    /**
     * Changes the autocommit mode for this session.
     *
     * @param autoCommit true to request autocommit; false to
     * request manual commit
     */
    public void setAutoCommit(boolean autoCommit);

    /**
     * Commits current transaction if any.
     */
    public void commit();

    /**
     * Rolls back current transaction if any.
     *
     * @param savepoint savepoint to roll back to, or null to rollback
     * entire transaction
     */
    public void rollback(FarragoSessionSavepoint savepoint);

    /**
     * Creates a new savepoint based on the current session state.
     *
     * @param name name to give new savepoint, or null
     * for anonymous savepoint
     *
     * @return new savepoint
     */
    public FarragoSessionSavepoint newSavepoint(String name);

    /**
     * Releases an existing savepoint.
     *
     * @param savepoint savepoint to release
     */
    public void releaseSavepoint(FarragoSessionSavepoint savepoint);

    /**
     * Analyzes an SQL expression, and returns information about it.  Used
     * when an expression is not going to be executed directly, but needs
     * to be validated as part of the definition of some containing object
     * such as a view.
     *
     * @param sql text of SQL expression
     *
     * @param paramRowType if non-null, expression is expected to be
     * a function body with these parameters; if null, expression is
     * expected to be a query
     *
     * @return FarragoSessionAnalyzedSql derived from the query
     */
    public FarragoSessionAnalyzedSql analyzeSql(
        String sql,
        RelDataType paramRowType);

    /**
     * Determines the class to use for runtime context.
     *
     * @return runtime context class, which must implement
     * {@link FarragoSessionRuntimeContext}
     */
    public Class getRuntimeContextClass();

    /**
     * Creates a new runtime context.  The object returned must be
     * assignable to the result of getRuntimeContextClass().
     *
     * @param params context initialization parameters
     *
     * @return new context
     */
    public FarragoSessionRuntimeContext newRuntimeContext(
        FarragoSessionRuntimeParams params);
}


// End FarragoSession.java

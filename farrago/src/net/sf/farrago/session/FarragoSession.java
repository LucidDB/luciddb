/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 2003-2009 John V. Sichi
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

import java.util.*;
import java.util.regex.*;

import javax.jmi.reflect.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.plugin.*;
import net.sf.farrago.util.*;

import org.eigenbase.reltype.*;


/**
 * FarragoSession represents an internal API to the Farrago database. It is
 * designed to serve as a basis for the implementation of standard API's such as
 * JDBC.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface FarragoSession
    extends FarragoAllocation
{
    //~ Methods ----------------------------------------------------------------

    /**
     * @return the factory which created this session
     */
    public FarragoSessionFactory getSessionFactory();

    /**
     * @return the current personality for this session
     */
    public FarragoSessionPersonality getPersonality();

    /**
     * Creates a new statement context within this session.
     *
     * @param paramDefFactory a factory for FarragoSessionStmtParamDef instances
     *
     * @return new statement context
     */
    public FarragoSessionStmtContext newStmtContext(
        FarragoSessionStmtParamDefFactory paramDefFactory);

    /**
     * Creates a new statement context within this session.
     *
     * @param paramDefFactory a factory for FarragoSessionStmtParamDef instances
     * @param rootStmtContext the root statement context for an internally
     * prepared statement; for an externally prepared statement, this will be
     * null
     *
     * @return new statement context
     */
    public FarragoSessionStmtContext newStmtContext(
        FarragoSessionStmtParamDefFactory paramDefFactory,
        FarragoSessionStmtContext rootStmtContext);

    /**
     * Creates a new SQL statement validator.
     *
     * @return new validator
     */
    public FarragoSessionStmtValidator newStmtValidator();

    /**
     * Creates a new privilege checker for a session. This checker ensures that
     * the session user has the right privileges on the objects which it
     * requests to operate on.
     *
     * <p>Because privilege checkers are stateful, one privilege checker should
     * be created for each statement.
     *
     * @return new privilege checker
     */
    public FarragoSessionPrivilegeChecker newPrivilegeChecker();

    /**
     * @return {@link FarragoSessionPrivilegeMap} for this session
     */
    public FarragoSessionPrivilegeMap getPrivilegeMap();

    /**
     * @return JDBC URL used to establish this session
     */
    public String getUrl();

    /**
     * @return repos accessed by this session
     */
    public FarragoRepos getRepos();

    /**
     * @return ClassLoader for loading plugins
     */
    public FarragoPluginClassLoader getPluginClassLoader();

    /**
     * @return list of installed {@link FarragoSessionModelExtension} instances
     */
    public List<FarragoSessionModelExtension> getModelExtensions();

    /**
     * @return whether this session is an internal session cloned from another
     * session
     */
    public boolean isClone();

    /**
     * @return whether this session has already been closed
     */
    public boolean isClosed();

    /**
     * @return whether this session was killed (which implies closed)
     */
    public boolean wasKilled();

    /**
     * Kills this session. A killed session is closed, so the implementation of
     * this method should insure that {@link #closeAllocation} is called. After
     * this method is called, {@link #wasKilled()} and {@link #isClosed()} will
     * return true.
     */
    public void kill();

    /**
     * Cancels execution of any statements on this session (but does not kill it
     * or them).
     */
    public void cancel();

    /**
     * @return whether this session currently has a transaction in progress
     */
    public boolean isTxnInProgress();

    /**
     * Gets the ID of the current transaction on this session, optionally
     * initiating a new transaction if none is currently active.
     *
     * @param createIfNeeded if true and no transaction is active, create a new
     * one
     *
     * @return transaction ID, or null if no transaction active and
     * !createIfNeeded
     */
    public FarragoSessionTxnId getTxnId(boolean createIfNeeded);

    /**
     * @return transaction manager for this session
     */
    public FarragoSessionTxnMgr getTxnMgr();

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
     * @return session index map
     */
    public FarragoSessionIndexMap getSessionIndexMap();

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
     * Overrides the index map associated with this session
     *
     * @param sessionIndexMap index map to set
     */
    public void setSessionIndexMap(FarragoSessionIndexMap sessionIndexMap);

    /**
     * Clones this session. TODO: document what this entails.
     *
     * @param inheritedVariables session variables to use for context in new
     * session, or null to inherit those of session being cloned
     *
     * @return cloned session.
     */
    public FarragoSession cloneSession(
        FarragoSessionVariables inheritedVariables);

    /**
     * Changes the autocommit mode for this session.
     *
     * @param autoCommit true to request autocommit; false to request manual
     * commit
     */
    public void setAutoCommit(boolean autoCommit);

    /**
     * Commits current transaction if any.
     */
    public void commit();

    /**
     * Ends the current transaction if session is in autocommit mode. Normally,
     * an attempt to commit or rollback in autocommit mode will cause an
     * exception; this method is for use by other components which need to
     * notify the session that some event (e.g. cursor close) is triggering an
     * autocommit boundary.
     *
     * @param commit true to commit; false to rollback
     */
    public void endTransactionIfAuto(boolean commit);

    /**
     * Rolls back current transaction if any.
     *
     * @param savepoint savepoint to roll back to, or null to rollback entire
     * transaction
     */
    public void rollback(FarragoSessionSavepoint savepoint);

    /**
     * Creates a new savepoint based on the current session state.
     *
     * @param name name to give new savepoint, or null for anonymous savepoint
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
     * Analyzes an SQL expression, and returns information about it. Used when
     * an expression is not going to be executed directly, but needs to be
     * validated as part of the definition of some containing object such as a
     * view.
     *
     * @param sql text of SQL expression
     * @param typeFactory factory for creating result and param types
     * @param paramRowType if non-null, expression is expected to be a function
     * body with these parameters; if null, expression is expected to be a query
     * @param optimize if true, run optimizer as part of analysis; otherwise,
     * skip optimization, returning less information
     *
     * @return FarragoSessionAnalyzedSql derived from the query
     */
    public FarragoSessionAnalyzedSql analyzeSql(
        String sql,
        RelDataTypeFactory typeFactory,
        RelDataType paramRowType,
        boolean optimize);

    /**
     * Executes a LURQL query against the repository.
     *
     * @param lurql query string
     * @param argMap from parameter name (String) to argument value (typically
     * String or Set)
     *
     * @return collection of RefObjects retrieved by query
     */
    public Collection<RefObject> executeLurqlQuery(
        String lurql,
        Map<String, ?> argMap);

    /**
     * Returns a FarragoSessionInfo object which contains information on the
     * runtime state of the session (e.g., active statements).
     *
     * @return FarragoSessionInfo object
     */
    public FarragoSessionInfo getSessionInfo();

    /**
     * Sets the exclusion filter to use for planners created by this session.
     * See {@link org.eigenbase.relopt.RelOptPlanner#setRuleDescExclusionFilter}
     * for details.
     *
     * @param exclusionFilter pattern to match for exclusion; null to disable
     * filtering
     */
    public void setOptRuleDescExclusionFilter(Pattern exclusionFilter);

    /**
     * @return exclusion filter in effect for planners created by this session
     */
    public Pattern getOptRuleDescExclusionFilter();

    /**
     * Gets the warning queue for this session.
     *
     * @return warning queue
     */
    public FarragoWarningQueue getWarningQueue();

    /**
     * Disables subquery reduction for the current session.
     */
    public void disableSubqueryReduction();

    /**
     * Retrieves the commit sequence number associated with a session's label,
     * if it's set.
     *
     * @return the commit sequence number of a session's label; null if the
     * session does not have a label setting
     */
    public Long getSessionLabelCsn();

    /**
     * Retrieves the creation timestamp for the session's label setting, if a
     * label setting is set.
     *
     * @return the creation timestamp; null if the session does not have a label
     * setting
     */
    public Timestamp getSessionLabelCreationTimestamp();

    /**
     * Flags this FarragoSession as being a loopback session. Loopback sessions
     * do not block server shutdown.
     */
    public void setLoopback();

    /**
     * Tests whether this session is a loopback session.
     *
     * @return true if this is a loopback session, false otherwise
     *
     * @see #setLoopback()
     */
    public boolean isLoopback();

    /**
     * Tests whether this is a reentrant session executing DML on behalf of
     * ALTER TABLE REBUILD.
     *
     * @return true if this session is doing ALTER TABLE REBUILD, false
     * otherwise
     */
    public boolean isReentrantAlterTableRebuild();

    /**
     * Tests whether this is a reentrant session executing DML on behalf of
     * ALTER TABLE ADD COLUMN.
     *
     * @return true if this session is doing ALTER TABLE ADD COLUMN, false
     * otherwise
     */
    public boolean isReentrantAlterTableAddColumn();
}

// End FarragoSession.java

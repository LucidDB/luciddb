/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
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

import net.sf.farrago.catalog.*;
import net.sf.farrago.ddl.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.namespace.util.*;

import org.eigenbase.jmi.*;
import org.eigenbase.oj.rex.*;
import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.reltype.*;
import org.eigenbase.resgen.*;
import org.eigenbase.resource.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.type.*;


/**
 * FarragoSessionPersonality defines the SPI for plugging in custom
 * FarragoSession behavior.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface FarragoSessionPersonality
    extends FarragoStreamFactoryProvider
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Gets the SQL operator table to use for validating a statement.
     *
     * @param preparingStmt statement being prepared
     *
     * @return table of builtin SQL operators and functions to use for
     * validation; user-defined functions are not included here
     */
    public SqlOperatorTable getSqlOperatorTable(
        FarragoSessionPreparingStmt preparingStmt);

    /**
     * Gets the implementation table to use for compiling a statement that uses
     * a Java calculator.
     *
     * @param preparingStmt statement being prepared
     *
     * @return table of implementations corresponding to result of {@link
     * #getSqlOperatorTable(FarragoSessionPreparingStmt)}
     */
    public OJRexImplementorTable getOJRexImplementorTable(
        FarragoSessionPreparingStmt preparingStmt);

    /**
     * Gets the component associated with the given Class object from the
     * personality. If the personality does not support (or recognize) the
     * component type, it returns null. The returned instance is guaranteed to
     * be of type <code>C</code> or a subclass.
     *
     * @param componentInterface the interface desired
     *
     * @return an implementation of <code>componentInterface</code> or null
     */
    public <C> C newComponentImpl(Class<C> componentInterface);

    /**
     * Gets the name of the local data server to use for tables when none is
     * specified by CREATE TABLE.
     *
     * @param stmtValidator validator for statement being prepared
     *
     * @return server name
     */
    public String getDefaultLocalDataServerName(
        FarragoSessionStmtValidator stmtValidator);

    /**
     * Tests whether this session personality implements ALTER TABLE ADD COLUMN
     * in an incremental fashion (only adding on the new column as opposed to
     * reformatting existing rows). For example, a column store can just create
     * a new vertical partition; a smart row store may be able to transform old
     * tuple formats during queries by filling in default values on the fly.
     *
     * @return true iff the incremental optimization is implemented
     */
    public boolean isAlterTableAddColumnIncremental();

    /**
     * Creates a new SQL parser.
     *
     * @param session session which will use the parser
     *
     * @return new parser
     */
    public FarragoSessionParser newParser(
        FarragoSession session);

    /**
     * Creates a new preparing statement tied to this session and its underlying
     * database. Used to construct and implement an internal query plan.
     *
     * @param stmtContext embracing stmt context, if any; otherwise, null.
     * @param rootStmtContext the root stmt context
     * @param stmtValidator generic stmt validator
     *
     * @return a new {@link FarragoSessionPreparingStmt}.
     */
    public FarragoSessionPreparingStmt newPreparingStmt(
        FarragoSessionStmtContext stmtContext,
        FarragoSessionStmtContext rootStmtContext,
        FarragoSessionStmtValidator stmtValidator);

    /**
     * Creates a new preparing statement tied to this session and its underlying
     * database. Used to construct and implement an internal query plan.
     *
     * @param stmtContext embracing stmt context, if any; otherwise, null.
     * @param stmtValidator generic stmt validator
     *
     * @return a new {@link FarragoSessionPreparingStmt}.
     */
    public FarragoSessionPreparingStmt newPreparingStmt(
        FarragoSessionStmtContext stmtContext,
        FarragoSessionStmtValidator stmtValidator);

    /**
     * @deprecated Use the two-arg version instead, passing null for
     * stmtContext.
     */
    public FarragoSessionPreparingStmt newPreparingStmt(
        FarragoSessionStmtValidator stmtValidator);

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
     * See {@link FarragoSessionModelExtension#defineDdlHandlers}.
     *
     * @param ddlValidator validator which will invoke handlers
     * @param handlerList receives handler objects in order in which they should
     */
    public void defineDdlHandlers(
        FarragoSessionDdlValidator ddlValidator,
        List<DdlHandler> handlerList);

    /**
     * Defines privileges allowed on various object types.
     *
     * @param map receives allowed privileges
     */
    public void definePrivileges(
        FarragoSessionPrivilegeMap map);

    /**
     * Creates a new planner.
     *
     * @param stmt stmt on whose behalf planner will operate
     * @param init whether to initialize default rules in new planner
     *
     * @return new planner
     */
    public FarragoSessionPlanner newPlanner(
        FarragoSessionPreparingStmt stmt,
        boolean init);

    /**
     * Defines listeners for events which occur during planning.
     *
     * @param planner planner to which listeners should be added
     */
    public void definePlannerListeners(FarragoSessionPlanner planner);

    /**
     * Determines the class to use for runtime context.
     *
     * @param stmt stmt on whose behalf planner will operate
     *
     * @return runtime context class, which must implement {@link
     * FarragoSessionRuntimeContext}
     */
    public Class getRuntimeContextClass(
        FarragoSessionPreparingStmt stmt);

    /**
     * Creates a new runtime context. The object returned must be assignable to
     * the result of getRuntimeContextClass().
     *
     * @param params context initialization parameters
     *
     * @return new context
     */
    public FarragoSessionRuntimeContext newRuntimeContext(
        FarragoSessionRuntimeParams params);

    /**
     * Creates a new type factory.
     *
     * @param repos a repository containing Farrago metadata
     *
     * @return a new type factory
     */
    public RelDataTypeFactory newTypeFactory(
        FarragoRepos repos);

    /**
     * Loads variables from the session personality into a session variables
     * object. Each personality uses on its own variables. This method allows
     * the personality to declare its variables and set default values for them.
     * If any variables already have values, then they will not be overwritten.
     *
     * <p>This method should be called when initializing a new session or when
     * loading a new session personality for an existing session. This method
     * "leaves a mark", as it has the side effect of permanently updating the
     * session variables. Even if the session personality is swapped out, the
     * changes will remain.
     *
     * @param variables the session variables object
     */
    public void loadDefaultSessionVariables(
        FarragoSessionVariables variables);

    /**
     * Creates a set of session variables for use in a cloned session. Default
     * implementation is to just return variables.cloneVariables(), but
     * personalities may override, e.g. to reset some variables which should
     * never be inherited.
     *
     * @param variables set of variables to be inherited
     *
     * @return result of variable inheritance
     */
    public FarragoSessionVariables createInheritedSessionVariables(
        FarragoSessionVariables variables);

    /**
     * Checks whether a parameter value is appropriate for a session variable
     * and, if the value is appropriate, sets the session variable. If an error
     * is encountered, then the method throws an {@link
     * org.eigenbase.util.EigenbaseException}. Possible errors include when no
     * session variable has the specified name, when a non-numeric value was
     * specified for a numeric variable, when a directory does not exist, or
     * other errors.
     *
     * @param ddlValidator a ddl statement validator
     * @param variables a session variables object
     * @param name name of the session variable to be validated
     * @param value value to set for the session variable
     */
    public void validateSessionVariable(
        FarragoSessionDdlValidator ddlValidator,
        FarragoSessionVariables variables,
        String name,
        String value);

    /**
     * Creates a new processor for JMI queries.
     *
     * @param language query language (e.g. "LURQL")
     *
     * @return query processor, or null if language not supported
     */
    public JmiQueryProcessor newJmiQueryProcessor(String language);

    // TODO jvs 15-Oct-2005: replace this with the more generic
    // supportsFeature() method below
    /**
     * Returns whether a type is valid in this database.
     *
     * @param type Type
     *
     * @return whether a type is valid in this database
     */
    public boolean isSupportedType(SqlTypeName type);

    /**
     * Tests whether a feature is supported in this personality.
     *
     * @param feature {@link EigenbaseResource}  resource definition
     * representing the feature to be tested
     *
     * @return true iff feature is supported
     */
    public boolean supportsFeature(ResourceDefinition feature);

    /**
     * Tests whether this personality wants original SQL to be preserved for
     * dependent objects where possible during the revalidation triggered by
     * CREATE OR REPLACE.
     *
     * @return true iff an attempt should be made to preserve original SQL
     */
    public boolean shouldReplacePreserveOriginalSql();

    /**
     * Gives this personality a chance to register one or more {@link
     * RelMetadataProvider}s in the chain which will be used to answer
     * relational expression metadata queries during optimization. Personalities
     * which define their own relational expressions will generally need to
     * supply corresponding metadata providers.
     *
     * @param chain receives personality's custom providers, if any
     */
    public void registerRelMetadataProviders(ChainedRelMetadataProvider chain);

    /**
     * Gives this personality the opportunity to retrieve rowcount information
     * returned by a DML operation.
     *
     * @param resultSet result set returned by DML operation
     * @param rowCounts list of rowcounts returned by the DML operation
     * @param tableModOp table modification operation that caused the rowcounts
     * to be modified
     */
    public void getRowCounts(
        ResultSet resultSet,
        List<Long> rowCounts,
        TableModificationRel.Operation tableModOp)
        throws SQLException;

    /**
     * Gives this personality the opportunity to update rowcount information in
     * the catalog tables for a specified table as a result of a particular DML
     * operation.
     *
     * @param session session that needs to update rowcounts
     * @param tableName fully qualified table name for which rowcounts will be
     * updated
     * @param rowCounts list of row counts returned by the DML statement
     * @param tableModOp table modification operation that caused the rowcounts
     * to be modified
     * @param runningContext the currently running session context.
     *
     * @return number of rows affected by the DML operation
     */
    public long updateRowCounts(
        FarragoSession session,
        List<String> tableName,
        List<Long> rowCounts,
        TableModificationRel.Operation tableModOp,
        FarragoSessionRuntimeContext runningContext);

    /**
     * Gives this personality the opportunity to reset rowcount information in
     * the catalog tables for a specified table
     *
     * @param table column set corresponding to table
     */
    public void resetRowCounts(FemAbstractColumnSet table);

    /**
     * Gives the personality the opportunity to update an index root page when
     * the index is rebuilt
     *
     * @param index index whose root is being updated
     * @param wrapperCache cache for looking up data wrappers
     * @param baseIndexMap map for managing index storage
     * @param newRoot index's new root page
     */
    public void updateIndexRoot(
        FemLocalIndex index,
        FarragoDataWrapperCache wrapperCache,
        FarragoSessionIndexMap baseIndexMap,
        Long newRoot);
}

// End FarragoSessionPersonality.java

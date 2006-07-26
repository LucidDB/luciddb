/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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

import java.util.*;

import org.eigenbase.jmi.*;
import org.eigenbase.oj.rex.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.resgen.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.*;
import org.eigenbase.resource.EigenbaseResource;


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
     * Creates a new SQL parser.
     *
     * @param session session which will use the parser
     *
     * @return new parser
     */
    public FarragoSessionParser newParser(
        FarragoSession session);

    /**
     * Creates a new preparing statement tied to this session and its
     * underlying database. Used to construct and implement an internal query
     * plan.
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
     * @deprecated Use the two-arg version instead, passing null
     * for stmtContext.
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
     * be tried
     */
    public void defineDdlHandlers(
        FarragoSessionDdlValidator ddlValidator,
        List handlerList);

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

    // TODO jvs 6-Apr-2005:  get rid of this once Aspen stops using it
    public void validate(
        FarragoSessionStmtValidator stmtValidator,
        SqlNode sqlNode);

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
     * @param feature {@link EigenbaseResource} resource definition representing
     * the feature to be tested
     *
     * @return true iff feature is supported
     */
    public boolean supportsFeature(ResourceDefinition feature);

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
}

// End FarragoSessionPersonality.java

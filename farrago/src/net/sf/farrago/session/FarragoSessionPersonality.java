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

import org.eigenbase.oj.rex.*;
import org.eigenbase.sql.*;

import java.util.*;

/**
 * FarragoSessionPersonality defines the SPI for plugging in custom
 * FarragoSession behavior.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface FarragoSessionPersonality extends FarragoStreamFactoryProvider
{
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
     * Gets the implementation table to use for compiling a statement.
     *
     * @param preparingStmt statement being prepared
     *
     * @return table of implementations corresponding to result
     * of {@link #getSqlOperatorTable}
     */
    public OJRexImplementorTable getOJRexImplementorTable(
        FarragoSessionPreparingStmt preparingStmt);
    
    /**
     * Gets the name of the local data server to use for tables when none
     * is specified by CREATE TABLE.
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
     * Creates a new validator for DDL commands.
     *
     * @param stmtValidator generic stmt validator
     *
     * @return new validator
     */
    public FarragoSessionDdlValidator newDdlValidator(
        FarragoSessionStmtValidator stmtValidator);

    /**
     * Defines the handlers to be used to validate and execute DDL actions
     * for various object types.  See {@link FarragoSessionDdlHandler}
     * for an explanation of how to define the handler objects in this
     * list.  Optionally, may also define drop rules.
     *
     * @param ddlValidator validator which will invoke handlers
     *
     * @param handlerList receives handler objects in order in which they should
     * be tried
     */
    public void defineDdlHandlers(
        FarragoSessionDdlValidator ddlValidator,
        List handlerList);

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
     * @return runtime context class, which must implement
     * {@link FarragoSessionRuntimeContext}
     */
    public Class getRuntimeContextClass(
        FarragoSessionPreparingStmt stmt);

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
    
    // TODO jvs 6-Apr-2005:  get rid of this once Aspen stops using it
    public void validate(
        FarragoSessionStmtValidator stmtValidator,
        SqlNode sqlNode);
}

// End FarragoSessionPersonality.java

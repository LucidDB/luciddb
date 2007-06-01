/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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

import net.sf.farrago.catalog.*;
import net.sf.farrago.fennel.*;
import net.sf.farrago.type.*;
import net.sf.farrago.util.*;

import org.eigenbase.oj.rel.*;
import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.validate.*;
import org.eigenbase.sql2rel.*;


/**
 * FarragoSessionPreparingStmt represents the process of Farrago-specific
 * preparation of a single SQL statement (it's not a context for executing a
 * series of statements; for that, see {@link FarragoSessionStmtContext}). The
 * result is a {@link FarragoSessionExecutableStmt}.
 *
 * <p>FarragoSessionPreparingStmt has a fleeting lifetime, which is why its name
 * is in the progressive tense. Once its job is done, it should be discarded
 * (and can't be reused).
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface FarragoSessionPreparingStmt
    extends FarragoAllocation
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Prepares (translates and implements) a parsed query or DML statement, but
     * does not execute it.
     *
     * @param sqlNode top-level node of parsed statement
     * @param sqlNodeOriginal original form of sqlNode if it has been rewritten
     * by validation; otherwise, same as sqlNode
     *
     * @return prepared FarragoSessionExecutableStmt
     */
    public FarragoSessionExecutableStmt prepare(
        SqlNode sqlNode,
        SqlNode sqlNodeOriginal);

    /**
     * Analyzes an SQL expression, and returns information about it. Used when
     * an expression is not going to be executed directly, but needs to be
     * validated as part of the definition of some containing object such as a
     * view.
     *
     * @param sqlNode SQL expression to be analyzed
     * @param analyzedSql receives analysis result
     */
    public void analyzeSql(
        SqlNode sqlNode,
        FarragoSessionAnalyzedSql analyzedSql);

    /**
     * Sets up the environment the FarragoPreparingStmt needs in order to
     * implement (i.e. to generate code). When preparing a parsed query (by
     * calling {@link #prepare}) this happens automatically. But when
     * implementing a query presented as a query plan (by calling {@link
     * #implement}), you must call this method first, even before constructing
     * the query plan.
     */
    public void preImplement();

    /**
     * Performs post-validation work after SqlValidator has been called.
     *
     * @param validatedSqlNode output of SqlValidator
     */
    public void postValidate(SqlNode validatedSqlNode);

    /**
     * Implements a logical or physical query plan but does not execute it. You
     * must call {@link #preImplement()} first, in fact before constructing the
     * query plan.
     *
     * @param rootRel root of query plan (relational expression)
     * @param sqlKind SqlKind for the relational expression: only
     * SqlKind.Explain and SqlKind.Dml are special cases.
     * @param logical true for a logical query plan (still needs to be
     * optimized), false for a physical plan.
     *
     * @return prepared FarragoSessionExecutableStmt
     */
    public FarragoSessionExecutableStmt implement(
        RelNode rootRel,
        SqlKind sqlKind,
        boolean logical);

    /**
     * Test if the implementation may be saved for reuse. Called after the
     * statement has been prepared.
     */
    public boolean mayCacheImplementation();

    /**
     * Disables caching for the statement
     */
    public void disableStatementCaching();

    /**
     * @return generic stmt validator
     */
    public FarragoSessionStmtValidator getStmtValidator();

    /**
     * @return the SqlOperatorTable used for operator lookup during this stmt's
     * preparation; this includes both system-defined and user-defined functions
     */
    public SqlOperatorTable getSqlOperatorTable();

    /**
     * @return the SqlValidator for this statement (creating it if it does not
     * yet exist)
     */
    public SqlValidator getSqlValidator();

    /**
     * @return the SqlToRelConverter used by this stmt (creating it if it does
     * not yet exist)
     */
    public SqlToRelConverter getSqlToRelConverter();

    /**
     * @return the RelOptCluster used by this stmt (possibly creating the
     * SqlToRelConverter as a side effect if it does not yet exist).
     */
    public RelOptCluster getRelOptCluster();

    /**
     * @return repos for this stmt
     */
    public FarragoRepos getRepos();

    /**
     * @return handle to Fennel database accessed by this stmt
     */
    public FennelDbHandle getFennelDbHandle();

    /**
     * @return type factory for this stmt
     */
    public FarragoTypeFactory getFarragoTypeFactory();

    /**
     * @return FarragoSessionIndexMap to use for accessing index storage
     */
    public FarragoSessionIndexMap getIndexMap();

    /**
     * @return session which invoked statement preparation
     */
    public FarragoSession getSession();

    /**
     * @return SQL text of the statement being prepared
     */
    public String getSql();

    /**
     * Obtains an implementor for relational expressions.
     *
     * @param rexBuilder RexBuilder to use for constructing row expressions
     *
     * @return new expression implementor
     */
    public JavaRelImplementor getRelImplementor(RexBuilder rexBuilder);

    /**
     * Looks up a named ColumnSet and loads its optimizer representation.
     *
     * @param name name of ColumnSet
     *
     * @return optimizer representation, or null if not found
     */
    public RelOptTable loadColumnSet(SqlIdentifier name);
}

// End FarragoSessionPreparingStmt.java

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

import net.sf.farrago.catalog.*;
import net.sf.farrago.fennel.*;
import net.sf.farrago.type.*;
import net.sf.farrago.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql2rel.*;
import org.eigenbase.rex.*;
import org.eigenbase.oj.rel.*;


/**
 * FarragoSessionPreparingStmt represents the process of Farrago-specific
 * preparation of a single SQL statement (it's not a context for executing a
 * series of statements; for that, see {@link FarragoSessionStmtContext}).  The
 * result is a {@link FarragoSessionExecutableStmt}.
 *
 *<p>
 *
 * FarragoSessionPreparingStmt has a fleeting lifetime, which is why its name
 * is in the progressive tense.  Once its job is done, it should be discarded
 * (and can't be reused).
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface FarragoSessionPreparingStmt extends FarragoAllocation
{
    //~ Methods ---------------------------------------------------------------

    /**
     * Performs validation on an SQL statement.
     *
     * @param sqlNode unvalidated SQL statement
     *
     * @return validated SQL statement
     */
    public SqlNode validate(SqlNode sqlNode);

    /**
     * Prepares (translates and implements) a parsed query or DML statement, but
     * does not execute it.
     *
     * @param sqlNode top-level node of parsed statement
     * @return prepared FarragoSessionExecutableStmt
     */
    public FarragoSessionExecutableStmt prepare(SqlNode sqlNode);

    /**
     * Partially prepares this statement for use as a view definition.
     *
     * @param info receives view info
     */
    public void prepareViewInfo(
        SqlNode sqlNode,
        FarragoSessionViewInfo info);

    /**
     * Sets up the environment the FarragoPreparingStmt needs in order to
     * implement (i.e. to generate code).  When preparing a parsed query (by
     * calling {@link #prepare}) this happens automatically.  But when
     * implementing a query presented as a query plan (by calling {@link
     * #implement}), you must call this method
     * first, even before constructing the query plan.
     */
    public void preImplement();

    /**
     * Implements a logical or physical query plan but does not execute it.
     * You must call {@link #preImplement()} first, in fact before constructing
     * the query plan.
     *
     * @param rootRel root of query plan (relational expression)
     * @param sqlKind SqlKind for the relational expression: only
     *   SqlKind.Explain and SqlKind.Dml are special cases.
     * @param logical true for a logical query plan (still needs to be
     *   optimized), false for a physical plan.
     * @return prepared FarragoSessionExecutableStmt
     */
    public FarragoSessionExecutableStmt implement(
        RelNode rootRel,
        SqlKind sqlKind,
        boolean logical);

    /**
     * @return generic stmt validator
     */
    public FarragoSessionStmtValidator getStmtValidator();

    /**
     * @return the SqlOperatorTable used for operator lookup during this stmt's
     * preparation
     */
    public SqlOperatorTable getSqlOperatorTable();

    /**
     * @return the SqlValidator for this statement (creating it if
     * it does not yet exist)
     */
    public SqlValidator getSqlValidator();

    /**
     * @return the SqlToRelConverter used by this stmt (creating it
     * if it does not yet exist)
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
     * Obtains an implementor for relational expressions.
     *
     * @param rexBuilder RexBuilder to use for constructing row expressions
     *
     * @return new expression implementor
     */
    public JavaRelImplementor getRelImplementor(RexBuilder rexBuilder);
}


// End FarragoSessionPreparingStmt.java

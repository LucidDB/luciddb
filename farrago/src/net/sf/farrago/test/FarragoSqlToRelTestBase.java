/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2006 The Eigenbase Project
// Copyright (C) 2006-2006 Disruptive Tech
// Copyright (C) 2006-2006 LucidEra, Inc.
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
package net.sf.farrago.test;

import java.io.*;

import junit.framework.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.db.*;
import net.sf.farrago.jdbc.engine.*;
import net.sf.farrago.query.*;
import net.sf.farrago.session.*;
import net.sf.farrago.util.*;

import openjava.ptree.*;

import org.eigenbase.oj.rel.*;
import org.eigenbase.oj.stmt.*;
import org.eigenbase.rel.*;
import org.eigenbase.relopt.hep.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql2rel.*;


/**
 * FarragoSqlToRelTestBase is an abstract base for Farrago tests which involve
 * conversion from SQL to relational algebra.
 *
 * <p>SQL statements to be translated use the normal test catalog.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class FarragoSqlToRelTestBase
    extends FarragoTestCase
{

    //~ Constructors -----------------------------------------------------------

    protected FarragoSqlToRelTestBase(String testName)
        throws Exception
    {
        super(testName);
    }

    //~ Methods ----------------------------------------------------------------

    protected abstract void initPlanner(FarragoPreparingStmt stmt)
        throws Exception;

    protected abstract void checkAbstract(
        FarragoPreparingStmt stmt,
        RelNode topRel)
        throws Exception;

    protected void checkQuery(
        String explainQuery)
        throws Exception
    {
        // hijack necessary internals
        FarragoJdbcEngineConnection farragoConnection =
            (FarragoJdbcEngineConnection) connection;
        FarragoDbSession session =
            (FarragoDbSession) farragoConnection.getSession();

        // guarantee release of any resources we allocate on the way
        FarragoCompoundAllocation allocations = new FarragoCompoundAllocation();
        FarragoReposTxnContext reposTxn = new FarragoReposTxnContext(repos);
        try {
            reposTxn.beginReadTxn();

            // create a private code cache: don't pollute the real
            // database code cache
            FarragoObjectCache objCache =
                new FarragoObjectCache(allocations, 0);

            // FarragoPreparingStmt does most of the work for us
            FarragoSessionStmtValidator stmtValidator =
                new FarragoStmtValidator(
                    repos,
                    session.getDatabase().getFennelDbHandle(),
                    session,
                    objCache,
                    objCache,
                    session.getSessionIndexMap(),
                    session.getDatabase().getDdlLockManager());
            allocations.addAllocation(stmtValidator);
            FarragoPreparingStmt stmt = new FarragoPreparingStmt(stmtValidator);
            stmt.enablePartialImplementation();

            initPlanner(stmt);

            // parse the EXPLAIN PLAN statement
            SqlParser sqlParser = new SqlParser(explainQuery);
            SqlNode sqlNode = sqlParser.parseStmt();

            // prepare it
            PreparedExplanation explanation =
                (PreparedExplanation) stmt.prepareSql(
                    sqlNode,
                    session.getPersonality().getRuntimeContextClass(stmt),
                    stmt.getSqlValidator(),
                    true);

            // dig out the top-level relational expression
            RelNode topRel = explanation.getRel();
            stmt.finalizeRelMetadata(topRel);

            checkAbstract(stmt, topRel);
        } finally {
            allocations.closeAllocation();
            reposTxn.commit();
        }
    }
}

// End FarragoSqlToRelTestBase.java

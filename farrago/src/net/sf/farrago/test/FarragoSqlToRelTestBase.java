/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/
package net.sf.farrago.test;

import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.db.*;
import net.sf.farrago.defimpl.FarragoDefaultSessionPersonality;
import net.sf.farrago.jdbc.engine.*;
import net.sf.farrago.query.*;
import net.sf.farrago.session.*;
import net.sf.farrago.util.*;

import org.eigenbase.oj.stmt.*;
import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.sql.*;


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

    protected void checkQuery(String explainQuery)
        throws Exception
    {
        addRulesAndCheckQuery(explainQuery, null);
    }

    protected void addRulesAndCheckQuery(
        final String explainQuery,
        List<RelOptRule> rules)
        throws Exception
    {
        // hijack necessary internals
        FarragoJdbcEngineConnection farragoConnection =
            (FarragoJdbcEngineConnection) connection;
        FarragoDbSession session =
            (FarragoDbSession) farragoConnection.getSession();
        final FarragoDefaultSessionPersonality personality =
            (FarragoDefaultSessionPersonality) session.getPersonality();

        // guarantee release of any resources we allocate on the way
        FarragoCompoundAllocation allocations = new FarragoCompoundAllocation();
        FarragoReposTxnContext reposTxn = repos.newTxnContext(true);
        try {
            reposTxn.beginReadTxn();

            // create a private code cache: don't pollute the real
            // database code cache
            FarragoObjectCache objCache =
                new FarragoObjectCache(
                    allocations,
                    0,
                    new FarragoLruVictimPolicy());

            // FarragoPreparingStmt does most of the work for us
            FarragoSessionStmtValidator stmtValidator =
                session.newStmtValidator(
                    objCache, objCache);
            allocations.addAllocation(stmtValidator);
            FarragoPreparingStmt stmt =
                (FarragoPreparingStmt) personality.newPreparingStmtForTesting(
                    explainQuery, stmtValidator);

            initPlanner(stmt);

            // add any additional rules needed in the planner
            if (rules != null) {
                for (RelOptRule rule : rules) {
                    stmt.getPlanner().addRule(rule);
                }
            }

            // parse the EXPLAIN PLAN statement
            final FarragoSessionParser parser =
                session.getPersonality().newParser(session);
            SqlNode sqlNode =
                (SqlNode) parser.parseSqlText(
                    stmtValidator, null, explainQuery, true);

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

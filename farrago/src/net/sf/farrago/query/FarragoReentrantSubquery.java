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
package net.sf.farrago.query;

import java.util.*;

import net.sf.farrago.fennel.rel.*;
import net.sf.farrago.session.*;

import org.eigenbase.rel.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.validate.*;
import org.eigenbase.sql2rel.*;


/**
 * Extends {@link FarragoReentrantStmtExecutor} to handle subqueries. The
 * subquery will be evaluated to a constant.
 *
 * <p>If the subquery is part of an EXISTS expression, the constant returned is
 * either TRUE or FALSE depending on whether the subquery returned zero or at
 * least one row.
 *
 * <p>Otherwise, the subquery must be a scalar subquery.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class FarragoReentrantSubquery
    extends FarragoReentrantStmtExecutor
{
    // ~ Instance fields -------------------------------------------------------

    //~ Instance fields --------------------------------------------------------

    private final SqlCall subq;
    private final SqlToRelConverter parentConverter;
    private final boolean isExists;
    private final boolean isExplain;

    // ~ Constructors ----------------------------------------------------------

    //~ Constructors -----------------------------------------------------------

    /**
     * Constructs a FarragoReentrantSubquery.
     *
     * @param subq the subquery to evaluate
     * @param parentConverter sqlToRelConverter associated with the parent query
     * @param isExists whether the subquery is part of an EXISTS expression
     * @param isExplain whether the subquery is part of an EXPLAIN PLAN
     * statement
     * @param results the resulting evaluated expressions
     */
    FarragoReentrantSubquery(
        SqlCall subq,
        SqlToRelConverter parentConverter,
        boolean isExists,
        boolean isExplain,
        List<RexNode> results)
    {
        super(
            FennelRelUtil.getPreparingStmt(parentConverter.getCluster())
                         .getRootStmtContext(),
            parentConverter.getRexBuilder(),
            results);
        FarragoSessionStmtContext rootContext = getRootStmtContext();
        if (rootContext != null) {
            rootContext.setSaveFirstTxnCsn();
        }
        if (!isExists) {
            assert subq.getKind() == SqlKind.SCALAR_QUERY;
        }
        this.subq = subq;
        this.parentConverter = parentConverter;
        this.isExists = isExists;
        this.isExplain = isExplain;
    }

    //~ Methods ----------------------------------------------------------------

    // ~ Methods ---------------------------------------------------------------

    protected void executeImpl()
        throws Exception
    {
        SqlCall call = (SqlCall) subq;
        SqlSelect select = (SqlSelect) call.getOperands()[0];

        // Convert the SqlNode tree to a RelNode tree; we need to do this
        // here so the RelNode tree is associated with the new preparing
        // stmt.
        FarragoPreparingStmt preparingStmt =
            (FarragoPreparingStmt) getPreparingStmt();
        SqlValidator validator = preparingStmt.getSqlValidator();
        SqlToRelConverter sqlConverter =
            preparingStmt.getSqlToRelConverter(
                validator,
                preparingStmt);
        preparingStmt.setParentStmt(
            FennelRelUtil.getPreparingStmt(parentConverter.getCluster()));

        // Add to the new converter any subqueries that have already been
        // converted by the parent so we can avoid re-executing them
        sqlConverter.addConvertedNonCorrSubqs(
            parentConverter.getMapConvertedNonCorrSubqs());
        RelNode plan = sqlConverter.convertQuery(select, true, true);

        // The subquery cannot have dynamic parameters
        if (sqlConverter.getDynamicParamCount() > 0) {
            failed = true;
            return;
        }

        List<RexNode> exprs = new ArrayList<RexNode>();
        RelDataType resultType = null;

        if (!isExists) {
            // Non-EXISTS subqueries need to be converted to single-value
            // subqueries
            plan = sqlConverter.convertToSingleValueSubq(select, plan);

            // Create a dummy expression to store the type of the result.
            // When setting the type, derive the type based on what a
            // scalar subquery should return and create the type from the
            // type factory of the parent query.
            resultType =
                call.getOperator().deriveType(
                    validator,
                    validator.getFromScope(select),
                    call);
            resultType = rexBuilder.getTypeFactory().copyType(resultType);
            exprs.add(rexBuilder.makeInputRef(resultType, 0));
        }

        plan = sqlConverter.decorrelate(select, plan);

        // If the subquery is part of an EXPLAIN PLAN statement, don't
        // execute the subquery, but instead just return a dynamic parameter
        // as a placeholder for the subquery result.  Otherwise, execute
        // the query to produce the constant expression.  Cast the expression
        // as needed so the type matches the expected result type.
        RexNode constExpr;
        if (isExplain) {
            if (isExists) {
                resultType =
                    rexBuilder.getTypeFactory().createSqlType(
                        SqlTypeName.BOOLEAN);
            }
            constExpr =
                rexBuilder.makeDynamicParam(
                    resultType,
                    parentConverter.getDynamicParamCountInExplain(true));
            results.add(constExpr);
        } else {
            executePlan(plan, exprs, isExists, false);
            if (!failed && !isExists) {
                constExpr = results.get(0);
                if (constExpr.getType() != resultType) {
                    constExpr =
                        rexBuilder.makeCast(
                            resultType,
                            constExpr);
                    results.set(0, constExpr);
                }
            }
        }
    }
}

// End FarragoReentrantSubquery.java

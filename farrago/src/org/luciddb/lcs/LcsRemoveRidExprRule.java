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
package org.luciddb.lcs;

import org.luciddb.session.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.type.*;


/**
 * LcsRemoveRidExprRule is a rule for converting rid expressions that appear in
 * a {@link ProjectRel} whose child is a {@link EmptyRel} into BIGINT null
 * literals. Since the child of the project produces no rows, it doesn't matter
 * what the rid expression returns, so long as the type of the constant matches
 * the original type of the rid expression.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class LcsRemoveRidExprRule
    extends RelOptRule
{
    public static final LcsRemoveRidExprRule instance =
        new LcsRemoveRidExprRule();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a LcsRemoveRidExprRule.
     */
    private LcsRemoveRidExprRule()
    {
        super(
            new RelOptRuleOperand(
                ProjectRel.class,
                new RelOptRuleOperand(EmptyRel.class, RelOptRule.ANY)));
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        // Only fire the rule if there is at least one rid expression
        // somewhere in one of the projection expressions.
        ProjectRel project = (ProjectRel) call.rels[0];
        RexNode [] projExprs = project.getChildExps();
        int projLen = projExprs.length;
        RexNode [] newProjExprs = new RexNode[projLen];
        RexBuilder rexBuilder = project.getCluster().getRexBuilder();
        RidExprConverter converter = new RidExprConverter(rexBuilder);
        for (int i = 0; i < projLen; i++) {
            newProjExprs[i] = projExprs[i].accept(converter);
        }
        if (!converter.foundRidExpr()) {
            return;
        }

        String [] fieldNames = new String[projLen];
        RelDataTypeField [] fields = project.getRowType().getFields();
        for (int i = 0; i < projLen; i++) {
            fieldNames[i] = fields[i].getName();
        }

        ProjectRel newProject =
            CalcRel.createProject(
                call.rels[1],
                newProjExprs,
                fieldNames);
        call.transformTo(newProject);
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * Shuttle that locates rid expressions and converts them to literals with
     * the value 0.
     */
    private class RidExprConverter
        extends RexShuttle
    {
        private RexBuilder rexBuilder;
        private boolean ridExprFound;

        public RidExprConverter(RexBuilder rexBuilder)
        {
            this.rexBuilder = rexBuilder;
            ridExprFound = false;
        }

        public RexNode visitCall(RexCall call)
        {
            if (call.getOperator() == LucidDbOperatorTable.lcsRidFunc) {
                ridExprFound = true;
                return rexBuilder.makeNullLiteral(SqlTypeName.BIGINT);
            } else {
                return super.visitCall(call);
            }
        }

        public boolean foundRidExpr()
        {
            return ridExprFound;
        }
    }
}

// End LcsRemoveRidExprRule.java

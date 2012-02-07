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

import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.type.*;


/**
 * LcsTableDeleteRule is a rule for converting an abstract {@link
 * TableModificationRel} into a corresponding {@link LcsTableDeleteRel}.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class LcsTableDeleteRule
    extends RelOptRule
{
    public static final LcsTableDeleteRule instance =
        new LcsTableDeleteRule();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a LcsTableDeleteRule.
     */
    private LcsTableDeleteRule()
    {
        super(
            new RelOptRuleOperand(
                TableModificationRel.class,
                new RelOptRuleOperand(ProjectRel.class, ANY)));
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public CallingConvention getOutConvention()
    {
        return FennelRel.FENNEL_EXEC_CONVENTION;
    }

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        TableModificationRel tableModification =
            (TableModificationRel) call.rels[0];

        if (!(tableModification.getTable() instanceof LcsTable)) {
            return;
        }

        if (!tableModification.isFlattened()) {
            return;
        }

        if (!tableModification.isDelete()) {
            return;
        }

        ProjectRel origProj = (ProjectRel) call.rels[1];

        // replace the project with one that projects out the rid column to
        // not nullable since that is what Fennel expects
        RexBuilder rexBuilder = origProj.getCluster().getRexBuilder();
        RexNode ridExpr =
            rexBuilder.makeCast(
                rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BIGINT),
                LucidDbSpecialOperators.makeRidExpr(rexBuilder, origProj));
        RexNode [] singletonRidExpr = { ridExpr };
        String [] fieldNames = { "rid" };

        ProjectRel projRel =
            (ProjectRel) CalcRel.createProject(
                origProj.getChild(),
                singletonRidExpr,
                fieldNames);

        RelNode fennelInput =
            mergeTraitsAndConvert(
                call.rels[0].getTraits(),
                FennelRel.FENNEL_EXEC_CONVENTION,
                projRel);
        if (fennelInput == null) {
            return;
        }

        LcsTableDeleteRel deleteRel =
            new LcsTableDeleteRel(
                tableModification.getCluster(),
                (LcsTable) tableModification.getTable(),
                tableModification.getConnection(),
                fennelInput,
                tableModification.getOperation(),
                tableModification.getUpdateColumnList());

        call.transformTo(deleteRel);
    }
}

// End LcsTableDeleteRule.java

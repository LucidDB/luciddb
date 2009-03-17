/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 LucidEra, Inc.
// Copyright (C) 2005-2007 The Eigenbase Project
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
package com.lucidera.lcs;

import com.lucidera.farrago.*;

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
    //~ Constructors -----------------------------------------------------------

    public LcsRemoveRidExprRule()
    {
        super(
            new RelOptRuleOperand(
                ProjectRel.class,
                new RelOptRuleOperand[] {
                    new RelOptRuleOperand(EmptyRel.class, RelOptRule.ANY)
                }));
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

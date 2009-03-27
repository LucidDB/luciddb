/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Copyright (C) 2005-2009 The Eigenbase Project
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

import com.lucidera.query.*;

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

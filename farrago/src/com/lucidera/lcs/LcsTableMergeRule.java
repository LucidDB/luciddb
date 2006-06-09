/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 LucidEra, Inc.
// Copyright (C) 2005-2005 The Eigenbase Project
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
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.sql.type.*;

import java.util.*;

/**
 * LcsTableMergeRule is a rule for converting an abstract {@link
 * TableModificationRel} into a corresponding {@link LcsTableMergeRel}.
 * 
 * @author Zelaine Fong
 * @version $Id$
 */
public class LcsTableMergeRule extends RelOptRule
{
    public LcsTableMergeRule()
    {
        super(new RelOptRuleOperand(
                TableModificationRel.class,
                new RelOptRuleOperand [] {
                    new RelOptRuleOperand(ProjectRel.class, null)
                }));
    }

    //~ Methods ---------------------------------------------------------------

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

        if (!tableModification.isMerge()) {
            return;
        }
        
        ProjectRel origProj = (ProjectRel) call.rels[1];
        RexNode[] origProjExprs = origProj.getProjectExps();
        
        // Replace the project with one that projects out the rid and 2 null
        // columns (to simulate a singleton bitmap entry) as well as the
        // expressions that make up a new insert target row.  The content of
        // insert target row depends on whether the rid is null or non-null.
        // In the case of the former, it corresponds to the target of the
        // INSERT substatement while in the latter, it corresponds to the
        // UPDATE.  These will be implemented using a CASE expression.
        RelDataTypeField[] targetFields =
            tableModification.getTable().getRowType().getFields();
        int nTargetFields = targetFields.length;
        RexNode[] projExprs = new RexNode[nTargetFields + 3];
        String[] fieldNames = new String[nTargetFields + 3];
        
        // create the rid expression on the target table and the null values
        RexBuilder rexBuilder = origProj.getCluster().getRexBuilder();
        RexNode nullLiteral = rexBuilder.makeNullLiteral(
            SqlTypeName.Varbinary, LcsIndexGuide.LbmBitmapSegMaxSize);
        int nSourceFields =
            origProj.getChild().getRowType().getFieldCount() - nTargetFields;
        RexNode ridExpr = LucidDbSpecialOperators.makeRidExpr(
            rexBuilder, origProj.getChild(), nSourceFields);
        RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
        RelDataType type = typeFactory.createTypeWithNullability(
            typeFactory.createSqlType(SqlTypeName.Bigint), true);
        RexNode nullableRidExpr = rexBuilder.makeCast(type, ridExpr);
        projExprs[0] = nullableRidExpr;
        projExprs[1] = nullLiteral;
        projExprs[2] = nullLiteral;
        fieldNames[0] = "rid";
        fieldNames[1] = "descriptor";
        fieldNames[2] = "segment";
        
        // create the when condition for the CASE expression
        RexNode whenExpr = rexBuilder.makeCall(
            SqlStdOperatorTable.isNullOperator, nullableRidExpr);
 
        List<String> updateList = tableModification.getUpdateColumnList();
        for (int i = 0; i < nTargetFields; i++) {
            // determine whether a target expression was specified for the
            // field in the UPDATE call
            int matchedSetExpr = updateList.indexOf(targetFields[i].getName());
            RexNode updateExpr;
            if (matchedSetExpr != -1) {
                updateExpr = origProjExprs[nTargetFields * 2 + matchedSetExpr];
            } else {
                updateExpr = origProjExprs[nTargetFields + i];
            }
            
            projExprs[i + 3] = rexBuilder.makeCall(
                SqlStdOperatorTable.caseOperator,
                new RexNode[] { whenExpr, origProjExprs[i], updateExpr }); 
            fieldNames[i + 3] = targetFields[i].getName();
        }

        ProjectRel projRel = (ProjectRel) CalcRel.createProject(
            origProj.getChild(), projExprs, fieldNames);
        
        RelNode fennelInput =
            mergeTraitsAndConvert(
                call.rels[0].getTraits(), FennelRel.FENNEL_EXEC_CONVENTION,
                projRel);
        if (fennelInput == null) {
            return;
        }

        LcsTableMergeRel mergeRel =
            new LcsTableMergeRel(
                tableModification.getCluster(),
                (LcsTable) tableModification.getTable(),
                tableModification.getConnection(),
                fennelInput,
                tableModification.getOperation(),
                tableModification.getUpdateColumnList());

        call.transformTo(mergeRel);      
    }
}

// End LcsTableMergeRule

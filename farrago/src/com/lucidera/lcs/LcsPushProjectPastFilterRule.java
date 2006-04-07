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

import java.util.*;

import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;

/**
 * LcsPushProjectPastFilter implements the rule for pushing a projection past
 * a filter so the <code>LcsTableProjectionRule</code> can be applied.
 * 
 * <p>ProjectRel(FilterRel(LcsRowScanRel)) ->
 *      FilterRel(ProjectRel(LcsRowScanRel))
 * 
 * @author Zelaine Fong
 * @version $Id$
 */
public class LcsPushProjectPastFilterRule extends RelOptRule
{
//  ~ Constructors ----------------------------------------------------------

    public LcsPushProjectPastFilterRule()
    {
        super(new RelOptRuleOperand(
                ProjectRel.class,
                new RelOptRuleOperand [] {
                    new RelOptRuleOperand(FilterRel.class, 
                    new RelOptRuleOperand [] {
                        new RelOptRuleOperand(LcsRowScanRel.class, null)
                })}));
    }

    //~ Methods ---------------------------------------------------------------

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        ProjectRel origProj = (ProjectRel) call.rels[0];
        FilterRel filterRel = (FilterRel) call.rels[1];
        RexNode origFilter = filterRel.getCondition();
        LcsRowScanRel rowScan = (LcsRowScanRel) call.rels[2];

        RelDataTypeField[] scanFields = rowScan.getRowType().getFields();
        int nScanFields = scanFields.length;
        BitSet filterRefs = new BitSet(nScanFields);
        RelOptUtil.findRexInputRefs(origFilter, filterRefs);

        RexNode[] projFields = origProj.getChildExps();
        int projLength = projFields.length;
        BitSet projRefs = new BitSet(nScanFields);
        // all projection references must be references to individual fields
        for (int i = 0; i < projLength; i++) {
            if (!(projFields[i] instanceof RexInputRef)) {
                return;
            }
            projRefs.set(((RexInputRef) projFields[i]).getIndex());
        }

        // cannot push project if the filter references columns that aren't
        // in the projection
        if (!RelOptUtil.contains(projRefs, filterRefs)) {
            return;
        }

        // convert the filter to reference the projected columns
        int adjustments[] = new int[nScanFields];
        for (int i = 0; i < nScanFields; i++) {
            adjustments[i] = 0;
        }
        for (int i = 0; i < projLength; i++) {
            int index = ((RexInputRef) projFields[i]).getIndex();
            adjustments[index] = -(index - i);
        }
        RexNode newFilter =
            RelOptUtil.convertRexInputRefs(
                filterRel.getCluster().getRexBuilder(), origFilter,
                scanFields, adjustments);

        // create a filter on top of a project, on top of the original
        // rowscan
        String fieldNames[] = new String[projLength];
        for (int i = 0; i < projLength; i++) {
            fieldNames[i] = origProj.getRowType().getFields()[i].getName();
        }
        ProjectRel newProject =
            (ProjectRel) CalcRel.createProject(rowScan, projFields, fieldNames);
        RelNode filterInput =
            mergeTraitsAndConvert(
                rowScan.getTraits(), FennelRel.FENNEL_EXEC_CONVENTION,
                newProject);
        RelNode newFilterRel = CalcRel.createFilter(filterInput, newFilter);

        call.transformTo(newFilterRel);
    }
}

// End LcsPushProjectPastFilterRule.java

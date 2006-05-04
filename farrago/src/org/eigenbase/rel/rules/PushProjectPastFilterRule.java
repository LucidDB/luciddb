/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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

package org.eigenbase.rel.rules;

import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;

/**
 * PushProjectPastFilterRule implements the rule for pushing a projection past
 * a filter. 
 * 
 * @author Zelaine Fong
 * @version $Id$
 */
public class PushProjectPastFilterRule extends AbstractPushProjectRule
{
    //  ~ Constructors ---------------------------------------------------------

    public PushProjectPastFilterRule()
    {
        super(new RelOptRuleOperand(
                ProjectRel.class,
                new RelOptRuleOperand [] {
                    new RelOptRuleOperand(FilterRel.class, null)
                }));
    }

    //~ Methods ---------------------------------------------------------------

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        ProjectRel origProj = (ProjectRel) call.rels[0];
        FilterRel filterRel = (FilterRel) call.rels[1];
        RexNode origFilter = filterRel.getCondition();
        RelNode rel = filterRel.getChild();

        RelDataTypeField[] scanFields = rel.getRowType().getFields();
        int nScanFields = scanFields.length;

        RexNode[] origProjExprs = origProj.getChildExps();
        
        // locate all fields referenced in the projection and filter
        BitSet projRefs = new BitSet(nScanFields);
        locateAllRefs(origProjExprs, origFilter, projRefs);
        
        // if all fields are being projected, no point in proceeding
        // any further
        if (projRefs.cardinality() == nScanFields) {
            return;
        }

        // create a new projection referencing all fields referenced in 
        // either the project or the filter
        RexBuilder rexBuilder = origProj.getCluster().getRexBuilder();
        int newProjLength = projRefs.cardinality();
        RelNode newProject = createProjectInputRefs(
            rexBuilder, projRefs, scanFields, 0, newProjLength, rel);
       
        // convert the filter to reference the projected columns and create
        // a filter on top of the project just created
        int[] adjustments = getAdjustments(scanFields, projRefs);
        RexNode newFilter =
            RelOptUtil.convertRexInputRefs(
                rexBuilder, origFilter, scanFields, adjustments);
        RelNode newFilterRel = CalcRel.createFilter(newProject, newFilter);

        // put the original project on top of the filter, converting it to
        // reference the modified projection list
        ProjectRel topProject = createNewProject(
            origProj, scanFields, adjustments, rexBuilder, newFilterRel);
        
        call.transformTo(topProject);
    }
}

// End PushProjectPastFilterRule.java

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
 * AbstractPushProjectRule is a base class for implementing projection pushing
 * rules.
 * 
 * @author Zelaine Fong
 * @version $Id$
 */
public abstract class AbstractPushProjectRule extends RelOptRule
{
    public AbstractPushProjectRule(RelOptRuleOperand operand)
    {
        super(operand);
    }

    // implement RelOptRule
    public abstract void onMatch(RelOptRuleCall call);
    
    /**
     * Sets a bitmap with all references found in an array of projection
     * expressions and a filter
     * 
     * @param projExprs the array of projection expressions
     * @param filter the filter
     * @param projRefs the bitmap to be set
     */
    public void locateAllRefs(
        RexNode[] projExprs, RexNode filter, BitSet projRefs)
    {
        for (int i = 0; i < projExprs.length; i++) {
            RelOptUtil.findRexInputRefs(projExprs[i], projRefs);
        }

        // locate any additional fields referenced in the filter
        if (filter != null) {
            RelOptUtil.findRexInputRefs(filter, projRefs);
        }
    }
    
    /**
     * Creates a projection based on the inputs specified in a bitmap.
     * 
     * @param rexBuilder rex builder
     * @param projRefs bitmap containing input references that will be projected
     * @param relFields the fields that the projection will be referencing
     * @param offset first input in the bitmap that this projection can 
     * possibly reference
     * @param newProjLength number of expressions in the projection to be built
     * @param projChild child that the projection will be created on top of
     * @return created projection
     */
    public ProjectRel createProjectInputRefs(
        RexBuilder rexBuilder, BitSet projRefs, RelDataTypeField[] relFields,
        int offset, int newProjLength, RelNode projChild)
    {
        int refIdx = offset - 1;
        RexNode[] newProjExprs = new RexNode[newProjLength];
        String fieldNames[] = new String[newProjLength];
        for (int i = 0; i < newProjLength; i++) {
            refIdx = projRefs.nextSetBit(refIdx + 1);
            assert(refIdx >= 0);
            newProjExprs[i] = rexBuilder.makeInputRef(
                relFields[refIdx - offset].getType(), refIdx - offset);
            fieldNames[i] = relFields[refIdx - offset].getName();
        }
        
        return (ProjectRel) CalcRel.createProject(
            projChild, newProjExprs, fieldNames);
    }
    
    /**
     * Determines how much each input reference needs to be adjusted as a
     * result of projection
     * 
     * @param relFields the original input reference fields
     * @param projRefs bitmap containing the projected fields
     * @return array indicating how much each input needs to be adjusted by
     */
    public int[] getAdjustments(RelDataTypeField[] relFields, BitSet projRefs)
    {
        int adjustments[] = new int[relFields.length];
        int newIdx = 0;
        for (int pos = projRefs.nextSetBit(0); pos >= 0;
            pos = projRefs.nextSetBit(pos + 1))
        {
            adjustments[pos] = -(pos - newIdx);
            newIdx++;
        }
        return adjustments;
    }
    
    /**
     * Creates a new projection based on an original projection passed in,
     * adjusting all input refs based on an adjustment array passed in.
     * 
     * @param origProj the original projection on which this new project is
     * based
     * @param relFields the underlying fields referenced by the original project
     * @param adjustments array indicating how much each input reference should
     * be adjusted by
     * @param rexBuilder rex builder
     * @param projChild child of the new project
     * @return
     */
    public ProjectRel createNewProject(
        ProjectRel origProj, RelDataTypeField[] relFields,
        int[] adjustments, RexBuilder rexBuilder, RelNode projChild)
    {
        RexNode[] origProjExprs = origProj.getChildExps();
        int origProjLength = origProjExprs.length;
        RexNode[] projExprs = new RexNode[origProjLength];
        String[] fieldNames = new String[origProjLength];
        
        for (int i = 0; i < origProjLength; i++) {
            projExprs[i] =
                RelOptUtil.convertRexInputRefs(
                    rexBuilder, origProjExprs[i], relFields, adjustments);
            fieldNames[i] = origProj.getRowType().getFields()[i].getName();
        }
        ProjectRel projRel = (ProjectRel) CalcRel.createProject(
            projChild, projExprs, fieldNames);
        
        return projRel;
    }
}

// End AbstractPushProjectRule.java

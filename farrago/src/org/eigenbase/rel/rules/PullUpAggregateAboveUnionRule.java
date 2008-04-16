/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2002-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2003-2007 John V. Sichi
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

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;

/**
 * PullUpAggregateAboveUnionRule implements the rule for pulling {@link
 * AggregateRel}s beneath a {@link UnionRel} so two {@link AggregateRel}s
 * that are used to remove duplicates can be combined into a single
 * {@link AggregateRel}.
 * 
 * <p>This rule only handles cases where the {@link UnionRel}s still have
 * only two inputs.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class PullUpAggregateAboveUnionRule
    extends RelOptRule
{
    
    //~ Constructors -----------------------------------------------------------

    public PullUpAggregateAboveUnionRule()
    {
        super(
            new RelOptRuleOperand(
                AggregateRel.class,
                new RelOptRuleOperand[] {
                    new RelOptRuleOperand(
                        UnionRel.class,
                        new RelOptRuleOperand[] {
                            new RelOptRuleOperand(RelNode.class, null),
                            new RelOptRuleOperand(RelNode.class, null)
                        })}));
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        UnionRel unionRel = (UnionRel) call.rels[1];
        // If distincts haven't been removed yet, defer invoking this rule
        if (unionRel.isDistinct()) {
            return;
        }  
        
        AggregateRel topAggRel = (AggregateRel) call.rels[0];
        AggregateRel bottomAggRel;
        // We want to apply this rule on the pattern where the AggregateRel
        // is the second input into the UnionRel first.  Hence, that's why the
        // rule pattern matches on generic RelNodes rather than explicit
        // UnionRels.  By doing so, and firing this rule in a bottom-up order,
        // it allows us to only specify a single pattern for this rule.
        RelNode[] unionInputs = new RelNode[2];
        if (call.rels[3] instanceof AggregateRel) {
            bottomAggRel = (AggregateRel) call.rels[3];
            unionInputs[0] = call.rels[2];
            unionInputs[1] = call.rels[3].getInput(0);
        } else if (call.rels[2] instanceof AggregateRel) {
            bottomAggRel = (AggregateRel) call.rels[2];
            unionInputs[0] = call.rels[2].getInput(0);
            unionInputs[1] = call.rels[3];
        } else {
            return;
        }
        
        // Only pull up aggregates if they are there just to remove distincts
        if (!topAggRel.getAggCallList().isEmpty() ||
            !bottomAggRel.getAggCallList().isEmpty())
        {
            return;
        }
        
        UnionRel newUnionRel =
            new UnionRel(
                unionRel.getCluster(),
                unionInputs,
                true);    
        
        AggregateRel newAggRel =
            new AggregateRel(
                topAggRel.getCluster(),
                newUnionRel,
                topAggRel.getGroupCount(),
                topAggRel.getAggCallList());
        
        call.transformTo(newAggRel);
    }
}

// End PullUpAggregateAboveUnionRule.java

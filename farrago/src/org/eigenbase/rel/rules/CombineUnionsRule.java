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
 * CombineUnionsRule implements the rule for combining two non-distinct {@link
 * UnionRel}s into a single {@link UnionRel}.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class CombineUnionsRule
    extends RelOptRule
{
    //~ Constructors -----------------------------------------------------------

    public CombineUnionsRule()
    {
        super(
            new RelOptRuleOperand(
                UnionRel.class,
                new RelOptRuleOperand(RelNode.class, ANY),
                new RelOptRuleOperand(RelNode.class, ANY)));
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        UnionRel topUnionRel = (UnionRel) call.rels[0];
        UnionRel bottomUnionRel;

        // We want to combine the UnionRel that's in the second input first.
        // Hence, that's why the rule pattern matches on generic RelNodes
        // rather than explicit UnionRels.  By doing so, and firing this rule
        // in a bottom-up order, it allows us to only specify a single
        // pattern for this rule.
        if (call.rels[2] instanceof UnionRel) {
            bottomUnionRel = (UnionRel) call.rels[2];
        } else if (call.rels[1] instanceof UnionRel) {
            bottomUnionRel = (UnionRel) call.rels[1];
        } else {
            return;
        }

        // If distincts haven't been removed yet, defer invoking this rule
        if (topUnionRel.isDistinct() || bottomUnionRel.isDistinct()) {
            return;
        }

        // Combine the inputs from the bottom union with the other inputs from
        // the top union
        int nBottomUnionInputs = bottomUnionRel.getInputs().length;
        int nTopUnionInputs = topUnionRel.getInputs().length;
        RelNode [] unionInputs =
            new RelNode[nBottomUnionInputs + nTopUnionInputs - 1];
        if (call.rels[2] instanceof UnionRel) {
            assert (nTopUnionInputs == 2);
            unionInputs[0] = topUnionRel.getInput(0);
            System.arraycopy(
                bottomUnionRel.getInputs(),
                0,
                unionInputs,
                1,
                nBottomUnionInputs);
        } else {
            System.arraycopy(
                bottomUnionRel.getInputs(),
                0,
                unionInputs,
                0,
                nBottomUnionInputs);
            System.arraycopy(
                topUnionRel.getInputs(),
                1,
                unionInputs,
                nBottomUnionInputs,
                nTopUnionInputs - 1);
        }
        UnionRel newUnionRel =
            new UnionRel(
                topUnionRel.getCluster(),
                unionInputs,
                true);

        call.transformTo(newUnionRel);
    }
}

// End CombineUnionsRule.java

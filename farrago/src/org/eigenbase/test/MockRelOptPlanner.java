/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
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
package org.eigenbase.test;

import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;


/**
 * MockRelOptPlanner is a mock implementation of the {@link RelOptPlanner}
 * interface.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class MockRelOptPlanner
    extends AbstractRelOptPlanner
{
    //~ Instance fields --------------------------------------------------------

    private RelNode root;

    private RelOptRule rule;

    private RelNode transformationResult;

    //~ Methods ----------------------------------------------------------------

    // implement RelOptPlanner
    public void setRoot(RelNode rel)
    {
        this.root = rel;
    }

    // implement RelOptPlanner
    public RelNode getRoot()
    {
        return root;
    }

    // implement RelOptPlanner
    public boolean addRule(RelOptRule rule)
    {
        assert (this.rule == null) : "MockRelOptPlanner only supports a single rule";
        this.rule = rule;

        return false;
    }

    // implement RelOptPlanner
    public boolean removeRule(RelOptRule rule)
    {
        return false;
    }

    // implement RelOptPlanner
    public RelNode changeTraits(RelNode rel, RelTraitSet toTraits)
    {
        return rel;
    }

    // implement RelOptPlanner
    public RelNode findBestExp()
    {
        if (rule != null) {
            matchRecursive(root, null, -1);
        }
        return root;
    }

    /**
     * Recursively matches a rule.
     *
     * @param rel Relational expression
     * @param parent Parent relational expression
     * @param ordinalInParent Ordinal of relational expression among its
     *                        siblings
     * @return whether match occurred
     */
    private boolean matchRecursive(
        RelNode rel,
        RelNode parent,
        int ordinalInParent)
    {
        List<RelNode> bindings = new ArrayList<RelNode>();
        if (match(rule.getOperand(),
                rel,
                bindings))
        {
            MockRuleCall call =
                new MockRuleCall(
                    this,
                    rule.getOperand(),
                    bindings.toArray(new RelNode[bindings.size()]));
            if (rule.matches(call)) {
                rule.onMatch(call);
            }
        }

        if (transformationResult != null) {
            if (parent == null) {
                root = transformationResult;
            } else {
                parent.replaceInput(ordinalInParent, transformationResult);
            }
            return true;
        }

        RelNode [] children = rel.getInputs();
        for (int i = 0; i < children.length; ++i) {
            if (matchRecursive(children[i], rel, i)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Matches a relational expression to a rule.
     *
     * @param operand Root operand of rule
     * @param rel Relational expression
     * @param bindings Bindings, populated on successful match
     * @return whether relational expression matched rule
     */
    private boolean match(
        RelOptRuleOperand operand,
        RelNode rel,
        List<RelNode> bindings)
    {
        if (!operand.matches(rel)) {
            return false;
        }
        bindings.add(rel);
        RelOptRuleOperand [] childOperands = operand.getChildOperands();
        if (childOperands == null) {
            return true;
        }
        int n = childOperands.length;
        RelNode [] childRels = rel.getInputs();
        if (n != childRels.length) {
            return false;
        }
        for (int i = 0; i < n; ++i) {
            if (!match(childOperands[i],
                    childRels[i],
                    bindings))
            {
                return false;
            }
        }
        return true;
    }

    // implement RelOptPlanner
    public RelNode register(
        RelNode rel,
        RelNode equivRel)
    {
        return rel;
    }

    // implement RelOptPlanner
    public RelNode ensureRegistered(RelNode rel, RelNode equivRel)
    {
        return rel;
    }

    // implement RelOptPlanner
    public boolean isRegistered(RelNode rel)
    {
        return true;
    }

    //~ Inner Classes ----------------------------------------------------------

    private class MockRuleCall
        extends RelOptRuleCall
    {
        /**
         * Creates a MockRuleCall.
         *
         * @param planner Planner
         * @param operand Operand
         * @param rels List of matched relational expressions
         */
        MockRuleCall(
            RelOptPlanner planner,
            RelOptRuleOperand operand,
            RelNode [] rels)
        {
            super(
                planner, operand, rels,
                Collections.<RelNode, List<RelNode>>emptyMap());
        }

        // implement RelOptRuleCall
        public void transformTo(RelNode rel)
        {
            transformationResult = rel;
        }
    }
}

// End MockRelOptPlanner.java

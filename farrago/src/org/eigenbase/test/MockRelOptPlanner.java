/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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

import org.eigenbase.relopt.*;
import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.oj.rel.*;
import org.eigenbase.util.*;

import java.util.*;

/**
 * MockRelOptPlanner is a mock implementation of the {@link RelOptPlanner}
 * interface.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class MockRelOptPlanner implements RelOptPlanner
{
    private RelNode root;

    private RelOptRule rule;

    private RelNode transformationResult;

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
    public boolean addRelTraitDef(RelTraitDef relTraitDef)
    {
        return false;
    }

    // implement RelOptPlanner
    public boolean addRule(RelOptRule rule)
    {
        assert(this.rule == null)
            : "MockRelOptPlanner only supports a single rule";
        this.rule = rule;

        // TODO jvs 28-Mar-2006:  share common utility with other planners
        Walker operandWalker = new Walker(rule.getOperand());
        ArrayList operandsOfRule = new ArrayList();
        while (operandWalker.hasMoreElements()) {
            RelOptRuleOperand operand =
                (RelOptRuleOperand) operandWalker.nextElement();
            operand.setRule(rule);
            operand.setParent((RelOptRuleOperand) operandWalker.getParent());
            operandsOfRule.add(operand);
        }
        rule.operands =
            (RelOptRuleOperand [])
            operandsOfRule.toArray(RelOptRuleOperand.noOperands);
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
    public RelOptPlanner chooseDelegate()
    {
        return this;
    }

    // implement RelOptPlanner
    public RelNode findBestExp()
    {
        if (rule != null) {
            matchRecursive(root, null, -1);
        }
        return root;
    }

    private boolean matchRecursive(
        RelNode rel, RelNode parent, int ordinalInParent)
    {
        List<RelNode> bindings = new ArrayList<RelNode>();
        if (match(rule.getOperand(), rel, bindings)) {
            MockRuleCall call = new MockRuleCall(
                this,
                rule.getOperand(),
                bindings.toArray(RelNode.emptyArray));
            rule.onMatch(call);
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

    private boolean match(
        RelOptRuleOperand operand,
        RelNode rel,
        List<RelNode> bindings)
    {
        if (!operand.matches(rel)) {
            return false;
        }
        bindings.add(rel);
        Object [] childOperands = operand.getChildren();
        if (childOperands == null) {
            return true;
        }
        int n = childOperands.length;
        RelNode [] childRels = rel.getInputs();
        if (n != childRels.length) {
            return false;
        }
        for (int i = 0; i < n; ++i) {
            if (!match(
                    (RelOptRuleOperand) childOperands[i],
                    childRels[i],
                    bindings))
            {
                return false;
            }
        }
        return true;
    }

    // implement RelOptPlanner
    public RelOptCost makeCost(
        double dRows,
        double dCpu,
        double dIo)
    {
        return new MockRelOptCost();
    }

    // implement RelOptPlanner
    public RelOptCost makeHugeCost()
    {
        return new MockRelOptCost();
    }

    // implement RelOptPlanner
    public RelOptCost makeInfiniteCost()
    {
        return new MockRelOptCost();
    }

    // implement RelOptPlanner
    public RelOptCost makeTinyCost()
    {
        return new MockRelOptCost();
    }

    // implement RelOptPlanner
    public RelOptCost makeZeroCost()
    {
        return new MockRelOptCost();
    }

    // implement RelOptPlanner
    public RelOptCost getCost(RelNode rel)
    {
        return new MockRelOptCost();
    }

    // implement RelOptPlanner
    public RelNode register(
        RelNode rel,
        RelNode equivRel)
    {
        return rel;
    }

    // implement RelOptPlanner
    public RelNode ensureRegistered(RelNode rel)
    {
        return rel;
    }

    // implement RelOptPlanner
    public boolean isRegistered(RelNode rel)
    {
        return true;
    }

    // implement RelOptPlanner
    public void registerSchema(RelOptSchema schema)
    {
    }

    // implement RelOptPlanner
    public JavaRelImplementor getJavaRelImplementor(RelNode rel)
    {
        return null;
    }

    // implement RelOptPlanner
    public void addListener(RelOptListener newListener)
    {
    }

    // implement RelMetadataProvider
    public Object getRelMetadata(
        RelNode rel,
        String metadataQueryName,
        Object [] args)
    {
        return null;
    }

    // implement RelMetadataProvider
    public Object mergeRelMetadata(
        String metadataQueryName,
        Object md1,
        Object md2)
    {
        return null;
    }
    
    private class MockRuleCall extends RelOptRuleCall
    {
        MockRuleCall(
            RelOptPlanner planner,
            RelOptRuleOperand operand,
            RelNode [] rels)
        {
            super(planner, operand, rels);
        }
        
        // implement RelOptRuleCall
        public void transformTo(RelNode rel)
        {
            transformationResult = rel;
        }
    }
}

// End MockRelOptPlanner.java

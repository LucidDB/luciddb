/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2002-2006 Disruptive Tech
// Copyright (C) 2005-2006 The Eigenbase Project
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
package com.disruptivetech.farrago.volcano;

import java.util.*;
import java.util.logging.Level;

import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.RelVisitor;
import org.eigenbase.relopt.*;
import org.eigenbase.util.Util;

/**
 * <code>VolcanoRuleCall</code> implements the {@link RelOptRuleCall} interface
 * for VolcanoPlanner.
 */
public class VolcanoRuleCall extends RelOptRuleCall
{
    //~ Instance fields -------------------------------------------------------

    protected final VolcanoPlanner volcanoPlanner;

    //~ Constructors ----------------------------------------------------------

    protected VolcanoRuleCall(
        VolcanoPlanner volcanoPlanner,
        RelOptRuleOperand operand,
        RelNode [] rels)
    {
        super(volcanoPlanner, operand, rels);
        this.volcanoPlanner = volcanoPlanner;
    }

    VolcanoRuleCall(
        VolcanoPlanner planner,
        RelOptRuleOperand operand)
    {
        this(planner, operand, new RelNode[operand.getRule().operands.length]);
    }

    //~ Methods ---------------------------------------------------------------

    // implement RelOptRuleCall
    public void transformTo(RelNode rel)
    {
        try {
            // It's possible that rel is a subset or is already registered.
            // Is there still a point in continuing? Yes, because we might
            // discover that two sets of expressions are actually equivalent.

            // Make sure traits that the new rel doesn't know about are
            // propagated.
            RelTraitSet rels0Traits = rels[0].getTraits();
            new TraitPropagationVisitor(getPlanner(), rels0Traits).go(rel);

            if (tracer.isLoggable(Level.FINEST)) {
                tracer.finest("Rule " + getRule() + " arguments "
                    + RelOptUtil.toString(rels) + " created " + rel);
            }

            if (volcanoPlanner.listener != null) {
                RelOptListener.RuleProductionEvent event =
                    new RelOptListener.RuleProductionEvent(
                        volcanoPlanner,
                        rel,
                        this,
                        true);
                volcanoPlanner.listener.ruleProductionSucceeded(event);
            }
            
            Util.discard(getPlanner().register(rel, rels[0]));
            
            if (volcanoPlanner.listener != null) {
                RelOptListener.RuleProductionEvent event =
                    new RelOptListener.RuleProductionEvent(
                        volcanoPlanner,
                        rel,
                        this,
                        false);
                volcanoPlanner.listener.ruleProductionSucceeded(event);
            }
            
        } catch (Throwable e) {
            throw Util.newInternal(e,
                "Error occurred while applying rule " + getRule());
        }
    }

    /**
     * Called when all operands have matched.
     */
    protected void onMatch()
    {
        try {
            if (tracer.isLoggable(Level.FINEST)) {
                tracer.finest("Apply rule [" + getRule() + "] to ["
                    + RelOptUtil.toString(rels) + "]");
            }
            
            if (volcanoPlanner.listener != null) {
                RelOptListener.RuleAttemptedEvent event =
                    new RelOptListener.RuleAttemptedEvent(
                        volcanoPlanner,
                        rels[0],
                        this,
                        true);
                volcanoPlanner.listener.ruleAttempted(event);
            }
            
            getRule().onMatch(this);
            
            if (volcanoPlanner.listener != null) {
                RelOptListener.RuleAttemptedEvent event =
                    new RelOptListener.RuleAttemptedEvent(
                        volcanoPlanner,
                        rels[0],
                        this,
                        false);
                volcanoPlanner.listener.ruleAttempted(event);
            }
            
        } catch (Throwable e) {
            throw Util.newInternal(e, "Error while applying rule " +
                getRule() + ", args " + Arrays.asList(rels));
        }
    }

    /**
     * Applies this rule, with a given relexp in the first slot.
     *
     * @pre operand0.matches(rel)
     */
    void match(RelNode rel)
    {
        assert (getOperand0().matches(rel));
        final int solve = 0;
        int operandOrdinal = getOperand0().solveOrder[solve];
        this.rels[operandOrdinal] = rel;
        matchRecurse(solve + 1);
    }

    /**
     * @pre solve &gt; 0
     * @pre solve &lt;= rule.operands.length
     */
    private void matchRecurse(int solve)
    {
        if (solve == getRule().operands.length) {
            onMatch();
        } else {
            int operandOrdinal = getOperand0().solveOrder[solve];
            int previousOperandOrdinal = getOperand0().solveOrder[solve - 1];
            boolean ascending = operandOrdinal < previousOperandOrdinal;
            RelOptRuleOperand previousOperand =
                getRule().operands[previousOperandOrdinal];
            RelOptRuleOperand operand = getRule().operands[operandOrdinal];

            List successors;
            if (ascending) {
                assert (previousOperand.getParent() == operand);
                final RelNode childRel = rels[previousOperandOrdinal];
                RelSet set = volcanoPlanner.getSet(childRel);
                successors = set.getParentRels();
            } else {
                int parentOrdinal = operand.getParent().ordinalInRule;
                RelNode parentRel = rels[parentOrdinal];
                RelNode [] inputs = parentRel.getInputs();
                if (operand.ordinalInParent < inputs.length) {
                    RelSubset subset =
                        (RelSubset) inputs[operand.ordinalInParent];
                    successors = subset.rels;
                } else {
                    // The operand expects parentRel to have a certain number
                    // of inputs and it does not.
                    successors = Collections.EMPTY_LIST;
                }
            }

            for (int i = 0, n = successors.size(); i < n; i++) {
                RelNode rel = (RelNode) successors.get(i);
                if (!operand.matches(rel)) {
                    continue;
                }
                if (ascending) {
                    // We know that the previous operand was *a* child of
                    // its parent, but now check that it is the *correct*
                    // child
                    final RelSubset input =
                        (RelSubset) rel.getInput(previousOperand.ordinalInParent);
                    if (!input.rels.contains(rels[previousOperandOrdinal])) {
                        continue;
                    }
                }
                rels[operandOrdinal] = rel;
                matchRecurse(solve + 1);
            }
        }
    }

    private static class TraitPropagationVisitor
        extends RelVisitor
    {
        private final RelTraitSet baseTraits;
        private final RelOptPlanner planner;

        private TraitPropagationVisitor(
            RelOptPlanner planner, RelTraitSet baseTraits)
        {
            this.planner = planner;
            this.baseTraits = baseTraits;
        }

        public void visit(RelNode rel, int ordinal, RelNode parent)
        {
            if (rel instanceof RelSubset) {
                return;
            }

            if (planner.isRegistered(rel)) {
                return;
            }

            RelTraitSet relTraits = rel.getTraits();
            for (int i = 0; i < baseTraits.size(); i++) {
                if (i >= relTraits.size()) {
                    // Copy traits that the new rel doesn't know about.
                    relTraits.addTrait(baseTraits.getTrait(i));
                } else {
                    // Verify that the traits are from the same RelTraitDef
                    assert relTraits.getTrait(i).getTraitDef() ==
                        baseTraits.getTrait(i).getTraitDef();
                }
            }

            rel.childrenAccept(this);
        }
    }
}


// End VolcanoRuleCall.java

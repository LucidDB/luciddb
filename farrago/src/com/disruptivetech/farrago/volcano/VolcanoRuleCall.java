/*
// $Id$
// Farrago is a relational database management system.
// Copyright (C) 2002-2004 Disruptive Tech
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.*;
import org.eigenbase.trace.EigenbaseTrace;
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
        this(planner, operand, new RelNode[operand.rule.operands.length]);
    }

    //~ Methods ---------------------------------------------------------------

    // implement RelOptRuleCall
    public void transformTo(RelNode rel)
    {
        try {
            // Check that expression supports the desired convention.
            if (rel == rels[0]) {
                return;
            }
            if (rel instanceof RelSubset || planner.isRegistered(rel)) {
                return;
            }

            if (tracer.isLoggable(Level.FINEST)) {
                tracer.finest("Rule " + rule + " arguments "
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
            
            Util.discard(planner.register(rel, rels[0]));
            
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
                "Error occurred while applying rule " + rule);
        }
    }

    /**
     * Called when all operands have matched.
     */
    protected void onMatch()
    {
        try {
            if (tracer.isLoggable(Level.FINEST)) {
                tracer.finest("Apply rule [" + rule + "] to ["
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
            
            rule.onMatch(this);
            
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
            throw Util.newInternal(e, "Error while applying rule " + rule);
        }
    }

    RelOptRule getRule()
    {
        return operand0.rule;
    }

    /**
     * Applies this rule, with a given relexp in the first slot.
     *
     * @pre operand0.matches(rel)
     */
    void match(RelNode rel)
    {
        assert (operand0.matches(rel));
        final int solve = 0;
        int operandOrdinal = operand0.solveOrder[solve];
        this.rels[operandOrdinal] = rel;
        matchRecurse(solve + 1);
    }

    /**
     * @pre solve &gt; 0
     * @pre solve &lt;= rule.operands.length
     */
    private void matchRecurse(int solve)
    {
        if (solve == rule.operands.length) {
            onMatch();
        } else {
            int operandOrdinal = operand0.solveOrder[solve];
            int previousOperandOrdinal = operand0.solveOrder[solve - 1];
            boolean ascending = operandOrdinal < previousOperandOrdinal;
            RelOptRuleOperand previousOperand =
                rule.operands[previousOperandOrdinal];
            RelOptRuleOperand operand = rule.operands[operandOrdinal];

            List successors;
            if (ascending) {
                assert (previousOperand.parent == operand);
                final RelNode childRel = rels[previousOperandOrdinal];
                RelSet set = volcanoPlanner.getSet(childRel);
                successors = set.getParentRels();
            } else {
                int parentOrdinal = operand.parent.ordinalInRule;
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
}


// End VolcanoRuleCall.java

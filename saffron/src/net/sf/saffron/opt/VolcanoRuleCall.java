/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
// (C) Copyright 2003-2004 John V. Sichi
// You must accept the terms in LICENSE.html to use this software.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

package net.sf.saffron.opt;

import net.sf.saffron.core.SaffronPlanner;
import net.sf.saffron.opt.OptUtil;
import net.sf.saffron.rel.SaffronRel;
import net.sf.saffron.util.Util;

import openjava.tools.DebugOut;

import java.util.ArrayList;


/**
 * A <code>VolcanoRuleCall</code> is an invocation of a {@link VolcanoRule}
 * with a set of {@link SaffronRel relational expression}s as arguments.
 */
public class VolcanoRuleCall
{
    //~ Instance fields -------------------------------------------------------

    public final RuleOperand operand0;
    public final VolcanoRule rule;
    public final SaffronRel [] rels;

    //~ Constructors ----------------------------------------------------------

    protected VolcanoRuleCall(RuleOperand operand,SaffronRel [] rels)
    {
        this.operand0 = operand;
        this.rule = operand.rule;
        this.rels = rels;
        assert(rels.length == rule.operands.length);
    }

    VolcanoRuleCall(RuleOperand operand)
    {
        this(operand,new SaffronRel[operand.rule.operands.length]);
    }

    //~ Methods ---------------------------------------------------------------

    public SaffronPlanner getPlanner()
    {
        return rule.planner;
    }

    /**
     * Called by the rule whenever it finds a match.
     */
    public void transformTo(SaffronRel rel)
    {
        try {
            // Check that expression supports the desired convention.
            if (rel == rels[0]) {
                return;
            }
            if (rel instanceof RelSubset || rule.planner.isRegistered(rel)) {
                return;
            }
            DebugOut.println(
                "Rule " + rule + " arguments " + OptUtil.toString(rels)
                + " created " + rel);
            Util.discard(rule.planner.register(rel,rels[0]));
        } catch (Throwable e) {
            throw Util.newInternal(
                e,
                "Error occurred while applying rule " + rule);
        }
    }

    /**
     * Called when all operands have matched.
     */
    protected void onMatch()
    {
        try {
            if (DebugOut.getDebugLevel() > 2) {
                DebugOut.println(
                    "Apply rule [" + rule + "] to [" + OptUtil.toString(rels)
                    + "]");
            }
            rule.onMatch(this);
        } catch (Throwable e) {
            throw Util.newInternal(e,"Error while applying rule " + rule);
        }
    }

    VolcanoRule getRule()
    {
        return operand0.rule;
    }

    /**
     * Applies this rule, with a given relexp in the first slot.
     *
     * @pre operand0.matches(rel)
     */
    void match(SaffronRel rel)
    {
        assert(operand0.matches(rel));
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
            RuleOperand previousOperand =
                rule.operands[previousOperandOrdinal];
            RuleOperand operand = rule.operands[operandOrdinal];

            ArrayList successors;
            if (ascending) {
                assert(previousOperand.parent == operand);
                final SaffronRel childRel = rels[previousOperandOrdinal];
                RelSet set = rule.planner.getSet(childRel);
                successors = set.getParentRels();
            } else {
                int parentOrdinal = operand.parent.ordinalInRule;
                SaffronRel parentRel = rels[parentOrdinal];
                SaffronRel [] inputs = parentRel.getInputs();
                RelSubset subset = (RelSubset) inputs[operand.ordinalInParent];
                successors = subset.rels;
            }

            for (int i = 0,n = successors.size(); i < n; i++) {
                SaffronRel rel = (SaffronRel) successors.get(i);
                if (!operand.matches(rel)) {
                    continue;
                }
                if (ascending) {
                    // We know that the previous operand was *a* child of
                    // its parent, but now check that it is the *correct*
                    // child
                    final RelSubset input =
                        (RelSubset) rel.getInput(
                            previousOperand.ordinalInParent);
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

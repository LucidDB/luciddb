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
package org.eigenbase.relopt;

import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.util.*;


/**
 * A <code>RelOptRule</code> transforms an expression into another. It has a
 * list of {@link RelOptRuleOperand}s, which determine whether the rule can be
 * applied to a particular section of the tree.
 *
 * <p>The optimizer figures out which rules are applicable, then calls {@link
 * #onMatch} on each of them.</p>
 */
public abstract class RelOptRule
{
    //~ Instance fields --------------------------------------------------------

    /**
     * Description of rule, must be unique within planner. Default is the name
     * of the class sans package name, but derived classes are encouraged to
     * override.
     */
    protected String description;

    /**
     * Root of operand tree.
     */
    private final RelOptRuleOperand operand;

    /**
     * Flattened list of operands.
     */
    public RelOptRuleOperand [] operands;

    private RelTraitSet traits;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a rule.
     *
     * @param operand root operand, must not be null
     *
     * @pre operand != null
     */
    public RelOptRule(RelOptRuleOperand operand)
    {
        Util.pre(operand != null, "operand != null");
        this.operand = operand;
        this.description = guessDescription(getClass().getName());
        this.operands = flattenOperands(operand);
        assignSolveOrder();
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Creates a flattened list of this operand and its descendants in prefix
     * order.
     *
     * @param rootOperand Root operand
     *
     * @return Flattened list of operands
     */
    private RelOptRuleOperand [] flattenOperands(
        RelOptRuleOperand rootOperand)
    {
        List<RelOptRuleOperand> operandList =
            new ArrayList<RelOptRuleOperand>();

        // Flatten the operands into a list.
        rootOperand.setRule(this);
        rootOperand.setParent(null);
        rootOperand.ordinalInParent = 0;
        rootOperand.ordinalInRule = operandList.size();
        operandList.add(rootOperand);
        flattenRecurse(operandList, rootOperand);
        return operandList.toArray(new RelOptRuleOperand[operandList.size()]);
    }

    /**
     * Adds the operand and its descendants to the list in prefix order.
     *
     * @param operandList Flattened list of operands
     * @param parentOperand Parent of this operand
     */
    private void flattenRecurse(
        List<RelOptRuleOperand> operandList,
        RelOptRuleOperand parentOperand)
    {
        if (parentOperand.getChildOperands() == null) {
            return;
        }
        int k = 0;
        for (RelOptRuleOperand operand : parentOperand.getChildOperands()) {
            operand.setRule(this);
            operand.setParent(parentOperand);
            operand.ordinalInParent = k++;
            operand.ordinalInRule = operandList.size();
            operandList.add(operand);
            flattenRecurse(operandList, operand);
        }
    }

    /**
     * Builds each operand's solve-order. Start with itself, then its parent, up
     * to the root, then the remaining operands in prefix order.
     */
    private void assignSolveOrder()
    {
        for (RelOptRuleOperand operand : operands) {
            operand.solveOrder = new int[operands.length];
            int m = 0;
            for (RelOptRuleOperand o = operand; o != null; o = o.getParent()) {
                operand.solveOrder[m++] = o.ordinalInRule;
            }
            for (int k = 0; k < operands.length; k++) {
                boolean exists = false;
                for (int n = 0; n < m; n++) {
                    if (operand.solveOrder[n] == k) {
                        exists = true;
                    }
                }
                if (!exists) {
                    operand.solveOrder[m++] = k;
                }
            }

            // Assert: operand appears once in the sort-order.
            assert m == operands.length;
        }
    }

    /**
     * Returns the root operand of this rule
     *
     * @return the root operand of this rule
     */
    public RelOptRuleOperand getOperand()
    {
        return operand;
    }

    /**
     * Returns a flattened list of operands of this rule.
     *
     * @return flattened list of operands
     */
    public List<RelOptRuleOperand> getOperands()
    {
        return Collections.unmodifiableList(Arrays.asList(operands));
    }

    public int hashCode()
    {
        // Conventionally, hashCode() and equals() should use the same
        // criteria, whereas here we only look at the description. This is
        // okay, because the planner requires all rule instances to have
        // distinct descriptions.
        return description.hashCode();
    }

    public boolean equals(Object obj)
    {
        return (obj instanceof RelOptRule)
            && equals((RelOptRule) obj);
    }

    /**
     * Returns whether this rule is equal to another rule.
     *
     * <p>The base implementation checks that the rules have the same class and
     * that the operands are equal; derived classes can override.
     */
    protected boolean equals(RelOptRule that)
    {
        // Include operands and class in the equality criteria just in case
        // they have chosen a poor description.
        return this.description.equals(that.description)
            && (this.getClass() == that.getClass())
            && this.operand.equals(that.operand);
    }

    /**
     * Receives notification about a rule match. At the time that this method is
     * called, {@link RelOptRuleCall#rels call.rels} holds the set of relational
     * expressions which match the operands to the rule; <code>
     * call.rels[0]</code> is the root expression.
     *
     * <p>Typically a rule would check that the nodes are valid matches, creates
     * a new expression, then calls back {@link RelOptRuleCall#transformTo} to
     * register the expression.</p>
     */
    public abstract void onMatch(RelOptRuleCall call);

    /**
     * Returns the calling convention of the result of firing this rule, null if
     * not known.
     */
    public CallingConvention getOutConvention()
    {
        return null;
    }

    /**
     * Returns the trait which will be modified as a result of firing this
     * rule, or null if the rule is not a converter rule.
     */
    public RelTrait getOutTrait()
    {
        return null;
    }

    public String toString()
    {
        return description;
    }

    /**
     * Converts a relation expression to a give set of traits, if it does not
     * already have those traits. If the conversion is not possible, returns
     * null.
     *
     * @param rel Relexp to convert
     * @param toTraits desired traits
     *
     * @return a relational expression with the desired traits, or null if no
     * conversion is possible
     *
     * @post return == null || return.getTraits().matches(toTraits)
     */
    public static RelNode convert(RelNode rel, RelTraitSet toTraits)
    {
        RelOptPlanner planner = rel.getCluster().getPlanner();

        if (rel.getTraits().size() < toTraits.size()) {
            new RelTraitPropagationVisitor(planner, toTraits).go(rel);
        }

        RelTraitSet outTraits = RelOptUtil.clone(rel.getTraits());
        for (int i = 0; i < toTraits.size(); i++) {
            RelTrait toTrait = toTraits.getTrait(i);
            if (toTrait != null) {
                outTraits.setTrait(i, toTrait);
            }
        }

        if (rel.getTraits().matches(outTraits)) {
            return rel;
        }

        return planner.changeTraits(rel, outTraits);
    }

    /**
     * Creates a new RelTraitSet based on the given traits and converts the
     * relational expression to that trait set. Clones <code>baseTraits</code>
     * and merges <code>newTraits</code> with the cloned set, then converts rel
     * to that set. Normally, during a rule call, baseTraits are the traits of
     * the rel's parent and newTraits are the traits that the rule wishes to
     * guarantee.
     *
     * @param baseTraits base traits for converted rel
     * @param newTraits altered traits
     * @param rel the rel to convert
     *
     * @return converted rel or null if conversion could not be made
     */
    public static RelNode mergeTraitsAndConvert(
        RelTraitSet baseTraits,
        RelTraitSet newTraits,
        RelNode rel)
    {
        RelTraitSet traits = RelOptUtil.mergeTraits(baseTraits, newTraits);

        return convert(rel, traits);
    }

    /**
     * Creates a new RelTraitSet based on the given traits and converts the
     * relational expression to that trait set. Clones <code>baseTraits</code>
     * and merges <code>newTrait</code> with the cloned set, then converts rel
     * to that set. Normally, during a rule call, baseTraits are the traits of
     * the rel's parent and newTrait is the trait that the rule wishes to
     * guarantee.
     *
     * @param baseTraits base traits for converted rel
     * @param newTrait altered trait
     * @param rel the rel to convert
     *
     * @return converted rel or null if conversion could not be made
     */
    public static RelNode mergeTraitsAndConvert(
        RelTraitSet baseTraits,
        RelTrait newTrait,
        RelNode rel)
    {
        RelTraitSet traits = RelOptUtil.clone(baseTraits);

        traits.setTrait(
            newTrait.getTraitDef(),
            newTrait);

        return convert(rel, traits);
    }

    /**
     * Deduces a name for a rule by taking the name of its class and returning
     * the segment after the last '.' or '$'.
     *
     * @param className Name of the rule's class
     *
     * @return Last segment of the class
     */
    private static String guessDescription(String className)
    {
        String description = className;
        int punc =
            Math.max(
                className.lastIndexOf('.'),
                className.lastIndexOf('$'));
        if (punc >= 0) {
            // Examples:  * "com.foo.Bar" yields "Bar"  * "com.flatten.Bar$Baz"
            // yields "Baz";  * "com.foo.Bar$1" yields "1" (which as an integer
            // is an invalid     name, and writer of the rule is encouraged to
            // give it an     explicit name)
            description = className.substring(punc + 1);
        }
        return description;
    }
}

// End RelOptRule.java

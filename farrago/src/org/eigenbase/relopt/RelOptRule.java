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

package org.eigenbase.relopt;

import org.eigenbase.rel.RelNode;
import org.eigenbase.util.*;

import java.util.*;

/**
 * A <code>RelOptRule</code> transforms an expression into another. It has a
 * list of {@link RelOptRuleOperand}s, which determine whether the rule can be
 * applied to a particular section of the tree.
 *
 * <p>
 * The optimizer figures out which rules are applicable, then calls {@link
 * #onMatch} on each of them.
 * </p>
 */
public abstract class RelOptRule
{
    //~ Instance fields -------------------------------------------------------

    /**
     * Description of rule, must be unique within planner. Default is the name
     * of the class sans package name, but derived classes are encouraged to
     * override.
     */
    protected String description;

    /** Root of operand tree. */
    private final RelOptRuleOperand operand;

    /** Flattened list of operands. */
    public RelOptRuleOperand [] operands;

    private RelTraitSet traits;


    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a rule.
     *
     * @param operand root operand, must not be null
     * @pre operand != null
     */
    public RelOptRule(RelOptRuleOperand operand)
    {
        Util.pre(operand != null, "operand != null");
        this.operand = operand;
        this.description = guessDescription(getClass().getName());

        Walker<RelOptRuleOperand> operandWalker =
            new Walker<RelOptRuleOperand>(getOperand());
        List<RelOptRuleOperand> operandsOfRule =
            new ArrayList<RelOptRuleOperand>();
        while (operandWalker.hasNext()) {
            RelOptRuleOperand flattenedOperand = operandWalker.next();
            flattenedOperand.setRule(this);
            flattenedOperand.setParent(operandWalker.getParent());
            operandsOfRule.add(flattenedOperand);
        }
        operands = operandsOfRule.toArray(RelOptRuleOperand.noOperands);
    }

    //~ Methods ---------------------------------------------------------------

    public RelOptRuleOperand getOperand()
    {
        return operand;
    }

    public RelOptRuleOperand [] getOperands()
    {
        return operands;
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
        if (!(obj instanceof RelOptRule)) {
            return false;
        }
        return equals((RelOptRule) obj);
    }

    /**
     * Returns whether this rule is equal to another rule.
     *
     * <p>The base implementation checks that the rules have the same class
     * and that the operands are equal; derived classes can override.
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
     * Receives notification about a rule match. At the time that
     * this method is called, {@link RelOptRuleCall#rels call.rels} holds
     * the set of relational expressions which match the operands to the
     * rule; <code>call.rels[0]</code> is the root expression.
     *
     * <p>
     * Typically a rule would check that the nodes are valid matches, creates
     * a new expression, then calls back {@link RelOptRuleCall#transformTo}
     * to register the expression.
     * </p>
     */
    public abstract void onMatch(RelOptRuleCall call);

    /**
     * Returns the calling convention of the result of firing this rule, null
     * if not known.
     */
    public CallingConvention getOutConvention()
    {
        return null;
    }

    /**
     * Returns the set of traits of the result of firing this rule.  An empty
     * RelTraitSet is returned if the results are not known.
     */
    public RelTraitSet getOutTraits()
    {
        // REVIEW: SZ: 2/17/05: Lazy initialization here.  Some RelOptRule
        // implementations accept their "OutConvention" as a parameter to
        // their constructor.  If we did this in the constructor, we'd get the
        // wrong CallingConvention.  In the future, we could require that
        // the CallingConvention be passed to RelOptRule's constructor.
        if (traits == null) {
            traits = new RelTraitSet(new RelTrait[] { getOutConvention() });
        }

        // Changing the CallingConvention RelTrait via RelTraitSet.setTrait
        // is not supported!
        assert(getOutConvention() == traits.getTrait(0));

        return traits;       
    }

    public String toString()
    {
        return description;
    }


    /**
     * Converts a relation expression to a give set of traits, if it does not
     * already have those traits.  If the conversion is not possible, returns
     * null.
     *
     * @param rel Relexp to convert
     * @param toTraits desired traits
     * @return a relational expression with the desired traits, or null if no
     *         conversion is possible
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
        for(int i = 0; i < toTraits.size(); i++) {
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
     * relational expression to that trait set.  Clones <code>baseTraits</code>
     * and merges <code>newTraits</code> with the cloned set, then converts
     * rel to that set.  Normally, during a rule call, baseTraits are the
     * traits of the rel's parent and newTraits are the traits that the rule
     * wishes to guarantee.
     *
     * @param baseTraits base traits for converted rel
     * @param newTraits altered traits
     * @param rel the rel to convert
     * @return converted rel or null if conversion could not be made
     */
    public static RelNode mergeTraitsAndConvert(
        RelTraitSet baseTraits, RelTraitSet newTraits, RelNode rel)
    {
        RelTraitSet traits = RelOptUtil.mergeTraits(baseTraits, newTraits);

        return convert(rel, traits);
    }

    /**
     * Creates a new RelTraitSet based on the given traits and converts the
     * relational expression to that trait set.  Clones <code>baseTraits</code>
     * and merges <code>newTrait</code> with the cloned set, then converts
     * rel to that set.  Normally, during a rule call, baseTraits are the
     * traits of the rel's parent and newTrait is the trait that the rule
     * wishes to guarantee.
     *
     * @param baseTraits base traits for converted rel
     * @param newTrait altered trait
     * @param rel the rel to convert
     * @return converted rel or null if conversion could not be made
     */
    public static RelNode mergeTraitsAndConvert(
        RelTraitSet baseTraits, RelTrait newTrait, RelNode rel)
    {
        RelTraitSet traits = RelOptUtil.clone(baseTraits);

        traits.setTrait(newTrait.getTraitDef(), newTrait);

        return convert(rel, traits);
    }

    /**
     * Deduces a name for a rule by taking the name of its class and returning
     * the segment after the last '.' or '$'.
     *
     * @param className Name of the rule's class
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
            // Examples:
            //  * "com.foo.Bar" yields "Bar"
            //  * "com.foo.Bar$Baz" yields "Baz";
            //  * "com.foo.Bar$1" yields "1" (which as an integer is an invalid
            //     name, and writer of the rule is encouraged to give it an
            //     explicit name)
            description = className.substring(punc + 1);
        }
        return description;
    }
}


// End RelOptRule.java

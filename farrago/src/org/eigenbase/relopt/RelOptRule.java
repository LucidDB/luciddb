/*
// $Id$
// Package org.eigenbase is a class library of database components.
// Copyright (C) 2002-2004 Disruptive Tech
// Copyright (C) 2003-2004 John V. Sichi
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

package org.eigenbase.relopt;

import org.eigenbase.rel.RelNode;
import org.eigenbase.util.Util;


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
    public final RelOptRuleOperand operand;

    /** Flattened list of operands. */
    public RelOptRuleOperand [] operands;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a rule
     *
     * @param operand Root operand, must not be null
     * @pre operand != null
     */
    public RelOptRule(RelOptRuleOperand operand)
    {
        Util.pre(operand != null, "operand != null");
        this.operand = operand;
        this.description = guessDescription(getClass().getName());
    }

    //~ Methods ---------------------------------------------------------------

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
     * This method is called every time the rule matches. At the time that
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

    public String toString()
    {
        return description;
    }

    /**
     * Converts a relational expression to a given calling convention, if it
     * is not already of that convention. If the conversion is not possible,
     * returns null.
     *
     * <p>
     * The <code>stubborn</code> parameter controls how hard we try. Using
     * <code>true</code> causes an expression explosion. If the expression
     * you are converting appears as an operand of the rule, it is safe to
     * use <code>stubborn</code> = <code>false</code>: if the operand is
     * transformed to another type, the rule will be invoked again.
     * </p>
     *
     * @param rel Relexp to convert
     * @param toConvention Desired calling convention
     *
     * @return a relational expression of the desired calling convention, or
     *         null if no conversion is possible
     *
     * @post return == null || return.getConvention() == toConvention
     */
    protected static RelNode convert(
        RelNode rel,
        CallingConvention toConvention)
    {
        if (rel.getConvention() == toConvention) {
            return rel;
        }
        RelOptPlanner planner = rel.getCluster().planner;
        return planner.changeConvention(rel, toConvention);
    }

    private static String guessDescription(String className)
    {
        String description = className;
        int dot =
            Math.max(
                description.lastIndexOf('.'),
                description.lastIndexOf('$'));
        if (dot >= 0) {
            description = description.substring(dot + 1);
        }
        return description;
    }
}


// End RelOptRule.java

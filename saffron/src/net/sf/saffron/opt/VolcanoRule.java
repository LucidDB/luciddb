/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
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
import net.sf.saffron.rel.SaffronRel;


/**
 * A <code>VolcanoRule</code> transforms an expression into another. It has a
 * list of {@link RuleOperand}s, which determine whether the rule can be
 * applied to a particular section of the tree.
 * 
 * <p>
 * The optimizer figures out which rules are applicable, then calls {@link
 * #onMatch} on each of them.
 * </p>
 */
public abstract class VolcanoRule
{
    //~ Instance fields -------------------------------------------------------

    /**
     * Description of rule, must be unique within planner. Default is the name
     * of the class sans package name, but derived classes are encouraged to
     * override.
     */
    protected String description;

    /** Current planner; set by {@link VolcanoPlanner#addRule}. */
    protected VolcanoPlanner planner;

    /** Root of operand tree. */
    final RuleOperand operand;

    /** Flattened list of operands. */
    RuleOperand [] operands;

    //~ Constructors ----------------------------------------------------------

    public VolcanoRule(RuleOperand operand)
    {
        this.operand = operand;
        this.description = guessDescription(getClass().getName());
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * This method is called every time the rule matches. At the time that
     * this method is called, {@link VolcanoRuleCall#rels call.rels} holds
     * the set of relational expressions which match the operands to the
     * rule; <code>call.rels[0]</code> is the root expression.
     * 
     * <p>
     * Typically a rule would check that the nodes are valid matches, creates
     * a new expression, then calls back {@link VolcanoRuleCall#transformTo}
     * to register the expression.
     * </p>
     */
    public abstract void onMatch(VolcanoRuleCall call);

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
     * @param planner Planner
     * @param rel Relexp to convert
     * @param toConvention Desired calling convention
     *
     * @return a relational expression of the desired calling convention, or
     *         null if no conversion is possible
     *
     * @post return == null || return.getConvention() == toConvention
     */
    protected static SaffronRel convert(
        SaffronPlanner planner,
        SaffronRel rel,
        CallingConvention toConvention)
    {
        if (rel.getConvention() == toConvention) {
            return rel;
        }
        return planner.changeConvention(rel,toConvention);
    }

    /**
     * Returns the current {@link VolcanoPlanner}.
     */
    VolcanoPlanner getPlanner()
    {
        return planner;
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


// End VolcanoRule.java

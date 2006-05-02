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

import java.util.Arrays;

import org.eigenbase.rel.RelNode;
import org.eigenbase.util.Util;
import org.eigenbase.util.Walkable;


/**
 * A <code>RelOptRuleOperand</code> determines whether a {@link
 * org.eigenbase.relopt.RelOptRule} can be applied to a particular expression.
 *
 * <p>
 * For example, the rule to pull a filter up from the left side of a join
 * takes operands: <code>(Join (Filter) (Any))</code>.
 * </p>
 *
 * <p>
 * Note that <code>children</code> means different things if it is empty or it
 * is <code>null</code>: <code>(Join (Filter <b>()</b>) (Any))</code> means
 * that, to match the rule, <code>Filter</code> must have no operands.
 * </p>
 */
public class RelOptRuleOperand implements Walkable<RelOptRuleOperand>
{
    //~ Static fields/initializers --------------------------------------------

    public static final RelOptRuleOperand [] noOperands =
        new RelOptRuleOperand[0];

    //~ Instance fields -------------------------------------------------------

    private RelOptRuleOperand parent;
    private RelOptRule rule;

    // REVIEW jvs 29-Aug-2004: some of these are Volcano-specific and should be
    // factored out
    public int [] solveOrder;
    public int ordinalInParent;
    public int ordinalInRule;
    private final RelTraitSet traits;
    private final Class clazz;
    private final RelOptRuleOperand [] children;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates an operand which matches any {@link CallingConvention}.
     *
     * @pre clazz != null
     */
    public RelOptRuleOperand(
        Class clazz,
        RelOptRuleOperand [] children)
    {
        this(clazz, (RelTraitSet)null, children);
    }

    /**
     * Creates an operand which matches any {@link CallingConvention}.
     *
     * @pre clazz != null
     */
    public RelOptRuleOperand(
        Class clazz,
        CallingConvention convention,
        RelOptRuleOperand [] children)
    {
        this(clazz, new RelTraitSet(convention), children);
    }

    public RelOptRuleOperand(
        Class clazz,
        RelTraitSet traits,
        RelOptRuleOperand [] children)
    {
        assert (clazz != null);
        this.clazz = clazz;
        this.traits = traits;
        this.children = children;
        if (children != null) {
            for (int i = 0; i < this.children.length; i++) {
                this.children[i].parent = this;
            }
        }
    }

    //~ Methods ---------------------------------------------------------------

    public RelOptRuleOperand getParent()
    {
        return parent;
    }

    public void setParent(RelOptRuleOperand parent)
    {
        this.parent = parent;
    }

    public RelOptRule getRule()
    {
        return rule;
    }

    public void setRule(RelOptRule rule)
    {
        this.rule = rule;
    }
    
    public int hashCode()
    {
        int h = clazz.hashCode();
        h = Util.hash(
                h,
                traits.hashCode());
        h = Util.hashArray(h, children);
        return h;
    }

    public boolean equals(Object obj)
    {
        if (!(obj instanceof RelOptRuleOperand)) {
            return false;
        }
        RelOptRuleOperand that = (RelOptRuleOperand) obj;

        boolean equalTraits =
            this.traits != null
            ? this.traits.equals(that.traits)
            : that.traits == null;

        return (this.clazz == that.clazz)
            && equalTraits
            && Arrays.equals(this.children, that.children);
    }

    /**
     * @return relational expression class matched by this operand
     */
    public Class getMatchedClass()
    {
        return clazz;
    }

    // implement Walkable
    public RelOptRuleOperand [] getChildren()
    {
        return children;
    }

    /**
     * Returns whether a relational expression matches this operand. It must
     * be of the right class and calling convention.
     */
    public boolean matches(RelNode rel)
    {
        if (!clazz.isInstance(rel)) {
            return false;
        }
        if ((traits != null) && !rel.getTraits().matches(traits)) {
            return false;
        }
        return true;
    }
}


// End RelOptRuleOperand.java

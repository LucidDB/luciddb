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
 * A <code>RelOptRuleOperand</code> determines whether a {@link
 * org.eigenbase.relopt.RelOptRule} can be applied to a particular expression.
 *
 * <p>For example, the rule to pull a filter up from the left side of a join
 * takes operands: <code>(Join (Filter) (Any))</code>.</p>
 *
 * <p>Note that <code>children</code> means different things if it is empty or
 * it is <code>null</code>: <code>(Join (Filter <b>()</b>) (Any))</code> means
 * that, to match the rule, <code>Filter</code> must have no operands.</p>
 */
public class RelOptRuleOperand
{
    //~ Static fields/initializers ---------------------------------------------

    public static final RelOptRuleOperand [] noOperands =
        new RelOptRuleOperand[0];

    //~ Instance fields --------------------------------------------------------

    private RelOptRuleOperand parent;
    private RelOptRule rule;

    // REVIEW jvs 29-Aug-2004: some of these are Volcano-specific and should be
    // factored out
    public int [] solveOrder;
    public int ordinalInParent;
    public int ordinalInRule;
    private final RelTrait trait;
    private final Class clazz;
    private final RelOptRuleOperand [] children;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates an operand.
     *
     * @param clazz Class of relational expression to match (must not be null)
     *
     * @param children Child operands; or null, meaning match any number of
     * children
     */
    public RelOptRuleOperand(
        Class clazz,
        RelOptRuleOperand [] children)
    {
        this(clazz, null, children);
    }

    /**
     * Creates an operand which matches a given trait.
     *
     * @param clazz Class of relational expression to match (must not be null)
     *
     * @param trait Trait to match, or null to match any trait
     *
     * @param children Child operands; or null, meaning match any number of
     * children
     */
    public RelOptRuleOperand(
        Class clazz,
        RelTrait trait,
        RelOptRuleOperand [] children)
    {
        assert (clazz != null);
        this.clazz = clazz;
        this.trait = trait;
        this.children = children;
        if (children != null) {
            for (int i = 0; i < this.children.length; i++) {
                this.children[i].parent = this;
            }
        }
    }

    //~ Methods ----------------------------------------------------------------

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
            trait.hashCode());
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
            (this.trait != null) ? this.trait.equals(that.trait)
            : (that.trait == null);

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

    /**
     * Returns the child operands.
     *
     * @return child operands
     */
    public RelOptRuleOperand [] getChildOperands()
    {
        return children;
    }

    /**
     * Returns whether a relational expression matches this operand. It must be
     * of the right class and calling convention.
     */
    public boolean matches(RelNode rel)
    {
        if (!clazz.isInstance(rel)) {
            return false;
        }
        if ((trait != null) && !rel.getTraits().contains(trait)) {
            return false;
        }
        return true;
    }
}

// End RelOptRuleOperand.java

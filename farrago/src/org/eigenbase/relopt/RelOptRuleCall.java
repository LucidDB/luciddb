/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2002-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 2003-2009 John V. Sichi
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
import java.util.logging.*;

import org.eigenbase.rel.*;
import org.eigenbase.trace.*;


/**
 * A <code>RelOptRuleCall</code> is an invocation of a {@link RelOptRule} with a
 * set of {@link RelNode relational expression}s as arguments.
 */
public abstract class RelOptRuleCall
{
    //~ Static fields/initializers ---------------------------------------------

    protected static final Logger tracer = EigenbaseTrace.getPlannerTracer();

    //~ Instance fields --------------------------------------------------------

    private final RelOptRuleOperand operand0;
    private final Map<RelNode, List<RelNode>> nodeChildren;
    private final RelOptRule rule;
    public final RelNode [] rels;
    private final RelOptPlanner planner;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a RelOptRuleCall.
     *
     * @param planner Planner
     * @param operand Root operand
     * @param rels Array of relational expressions which matched each operand
     * @param nodeChildren For each node which matched with <code>
     * matchAnyChildren</code>=true, a list of the node's children
     */
    protected RelOptRuleCall(
        RelOptPlanner planner,
        RelOptRuleOperand operand,
        RelNode [] rels,
        Map<RelNode, List<RelNode>> nodeChildren)
    {
        this.planner = planner;
        this.operand0 = operand;
        this.nodeChildren = nodeChildren;
        this.rule = operand.getRule();
        this.rels = rels;
        assert (rels.length == rule.operands.length);
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Returns the root operand matched by this rule.
     *
     * @return root operand
     */
    public RelOptRuleOperand getOperand0()
    {
        return operand0;
    }

    /**
     * Returns the invoked planner rule.
     *
     * @return planner rule
     */
    public RelOptRule getRule()
    {
        return rule;
    }

    /**
     * Returns a list of matched relational expressions.
     *
     * @return matched relational expressions
     */
    public RelNode [] getRels()
    {
        return rels;
    }

    /**
     * Returns the children of a given relational expression node matched in a
     * rule.
     *
     * <p>If the operand which caused the match has {@link
     * RelOptRuleOperand#matchAnyChildren}=false, the children will have their
     * own operands and therefore be easily available in the array returned by
     * the {@link #getRels} method, so this method returns null.
     *
     * <p>This method is for {@link RelOptRuleOperand#matchAnyChildren}=true,
     * which is generally used when a node can have a variable number of
     * children, and hence where the matched children are not retrievable by any
     * other means.
     *
     * @param rel Relational expression
     *
     * @return Children of relational expression
     */
    public List<RelNode> getChildRels(RelNode rel)
    {
        return nodeChildren.get(rel);
    }

    /**
     * Returns the planner.
     *
     * @return planner
     */
    public RelOptPlanner getPlanner()
    {
        return planner;
    }

    /**
     * Called by the rule whenever it finds a match. The implementation of this
     * method will guarantee that the original relational expression (e.g.,
     * <code>this.rels[0]</code>) has its traits propagated to the new
     * relational expression (<code>rel</code>) and its unregistered children.
     * Any trait not specifically set in the RelTraitSet returned by <code>
     * rel.getTraits()</code> will be copied from <code>
     * this.rels[0].getTraitSet()</code>.
     */
    public abstract void transformTo(RelNode rel);
}

// End RelOptRuleCall.java

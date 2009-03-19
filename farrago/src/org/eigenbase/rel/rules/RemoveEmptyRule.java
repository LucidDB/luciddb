/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006-2009 The Eigenbase Project
// Copyright (C) 2006-2009 SQLstream, Inc.
// Copyright (C) 2006-2009 LucidEra, Inc.
// Portions Copyright (C) 2006-2009 John V. Sichi
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
package org.eigenbase.rel.rules;

import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;


/**
 * Collection of rules which remove sections of a query plan known never to
 * produce any rows.
 *
 * @author Julian Hyde
 * @version $Id$
 * @see EmptyRel
 */
public abstract class RemoveEmptyRule
    extends RelOptRule
{
    //~ Static fields/initializers ---------------------------------------------

    /**
     * Singleton instance of rule which removes empty children of a {@link
     * UnionRel}.
     *
     * <p>Examples:
     *
     * <ul>
     * <li>Union(Rel, Empty, Rel2) becomes Union(Rel, Rel2)
     * <li>Union(Rel, Empty, Empty) becomes Rel
     * <li>Union(Empty, Empty) becomes Empty
     * </ul>
     */
    public static final RemoveEmptyRule UNION_INSTANCE =
        new RemoveEmptyRule(
            new RelOptRuleOperand(
                UnionRel.class,
                null,
                true,
                new RelOptRuleOperand(
                    EmptyRel.class,
                    ANY)),
            "Union") {
            public void onMatch(RelOptRuleCall call)
            {
                UnionRel union = (UnionRel) call.rels[0];
                final List<RelNode> childRels = call.getChildRels(union);
                final List<RelNode> newChildRels = new ArrayList<RelNode>();
                for (RelNode childRel : childRels) {
                    if (!(childRel instanceof EmptyRel)) {
                        newChildRels.add(childRel);
                    }
                }
                assert newChildRels.size() < childRels.size() : "planner promised us at least one EmptyRel child";
                RelNode newRel;
                switch (newChildRels.size()) {
                case 0:
                    newRel =
                        new EmptyRel(
                            union.getCluster(),
                            union.getRowType());
                    break;
                case 1:
                    newRel =
                        RelOptUtil.createCastRel(
                            newChildRels.get(0),
                            union.getRowType(),
                            true);
                    break;
                default:
                    newRel =
                        new UnionRel(
                            union.getCluster(),
                            newChildRels.toArray(
                                new RelNode[newChildRels.size()]),
                            !union.isDistinct());
                    break;
                }
                call.transformTo(newRel);
            }
        };

    /**
     * Singleton instance of rule which converts a {@link ProjectRel} to empty
     * if its child is empty.
     *
     * <p>Examples:
     *
     * <ul>
     * <li>Project(Empty) becomes Empty
     * </ul>
     */
    public static final RemoveEmptyRule PROJECT_INSTANCE =
        new RemoveEmptyRule(
            new RelOptRuleOperand(
                ProjectRel.class,
                (RelTrait) null,
                new RelOptRuleOperand(
                    EmptyRel.class)),
            "Project") {
            public void onMatch(RelOptRuleCall call)
            {
                ProjectRel project = (ProjectRel) call.rels[0];
                call.transformTo(
                    new EmptyRel(
                        project.getCluster(),
                        project.getRowType()));
            }
        };

    /**
     * Singleton instance of rule which converts a {@link FilterRel} to empty if
     * its child is empty.
     *
     * <p>Examples:
     *
     * <ul>
     * <li>Filter(Empty) becomes Empty
     * </ul>
     */
    public static final RemoveEmptyRule FILTER_INSTANCE =
        new RemoveEmptyRule(
            new RelOptRuleOperand(
                FilterRel.class,
                (RelTrait) null,
                new RelOptRuleOperand(
                    EmptyRel.class)),
            "Filter") {
            public void onMatch(RelOptRuleCall call)
            {
                FilterRel filter = (FilterRel) call.rels[0];
                call.transformTo(
                    new EmptyRel(
                        filter.getCluster(),
                        filter.getRowType()));
            }
        };

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a RemoveEmptyRule.
     *
     * @param operand Operand
     * @param desc Description
     */
    private RemoveEmptyRule(RelOptRuleOperand operand, String desc)
    {
        super(operand);
        this.description = "RemoveEmptyRule:" + desc;
    }
}

// End RemoveEmptyRule.java

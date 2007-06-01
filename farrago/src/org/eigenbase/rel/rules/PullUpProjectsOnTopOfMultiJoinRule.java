/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2006 The Eigenbase Project
// Copyright (C) 2002-2006 Disruptive Tech
// Copyright (C) 2005-2006 LucidEra, Inc.
// Portions Copyright (C) 2003-2006 John V. Sichi
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

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;


/**
 * PullUpProjectsOnTopOfMultiJoinRule implements the rule for pulling {@link
 * ProjectRel}s that are on top of a {@link MultiJoinRel} and beneath a {@link
 * JoinRel} so the {@link ProjectRel} appears above the {@link JoinRel}. In the
 * process of doing so, also save away information about the respective fields
 * that are referenced in the expressions in the {@link ProjectRel} we're
 * pulling up, as well as the join condition, in the resultant {@link
 * MultiJoinRel}s
 *
 * <p>For example, if we have the following subselect:
 *
 * <pre>
 *      (select X.x1, Y.y1 from X, Y
 *          where X.x2 = Y.y2 and X.x3 = 1 and Y.y3 = 2)</pre>
 *
 * <p>The {@link MultiJoinRel} associated with (X, Y) associates x1 with X and
 * y1 with Y. Although x3 and y3 need to be read due to the filters, they are
 * not required after the row scan has completed and therefore are not saved.
 * The join fields, x2 and y2, are also tracked separately.
 *
 * <p>Note that by only pulling up projects that are on top of {@link
 * MultiJoinRel}s, we preserve projections on top of row scans.
 *
 * <p>See the superclass for details on restrictions regarding which {@link
 * ProjectRel}s cannot be pulled.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class PullUpProjectsOnTopOfMultiJoinRule
    extends PullUpProjectsAboveJoinRule
{
    // ~ Static fields/initializers --------------------------------------------

    //~ Static fields/initializers ---------------------------------------------

    public static final PullUpProjectsOnTopOfMultiJoinRule
        instanceTwoProjectChildren =
            new PullUpProjectsOnTopOfMultiJoinRule(
                new RelOptRuleOperand(
                    JoinRel.class,
                    new RelOptRuleOperand[] {
                        new RelOptRuleOperand(
                            ProjectRel.class,
                            new RelOptRuleOperand[] {
                                new RelOptRuleOperand(
                                    MultiJoinRel.class,
                                    null)
                            }),
                        new RelOptRuleOperand(
                            ProjectRel.class,
                            new RelOptRuleOperand[] {
                                new RelOptRuleOperand(
                                    MultiJoinRel.class,
                                    null)
                            })
                    }),
                "with two ProjectRel children");

    public static final PullUpProjectsOnTopOfMultiJoinRule
        instanceLeftProjectChild =
            new PullUpProjectsOnTopOfMultiJoinRule(
                new RelOptRuleOperand(
                    JoinRel.class,
                    new RelOptRuleOperand[] {
                        new RelOptRuleOperand(
                            ProjectRel.class,
                            new RelOptRuleOperand[] {
                                new RelOptRuleOperand(
                                    MultiJoinRel.class,
                                    null)
                            })
                    }),
                "with ProjectRel on left");

    public static final PullUpProjectsOnTopOfMultiJoinRule
        instanceRightProjectChild =
            new PullUpProjectsOnTopOfMultiJoinRule(
                new RelOptRuleOperand(
                    JoinRel.class,
                    new RelOptRuleOperand[] {
                        new RelOptRuleOperand(RelNode.class, null),
                        new RelOptRuleOperand(
                            ProjectRel.class,
                            new RelOptRuleOperand[] {
                                new RelOptRuleOperand(
                                    MultiJoinRel.class,
                                    null)
                            })
                    }),
                "with ProjectRel on right");

    //~ Constructors -----------------------------------------------------------

    public PullUpProjectsOnTopOfMultiJoinRule(RelOptRuleOperand rule, String id)
    {
        super(rule, id);
        description = "PullUpProjectsOnTopOfMultiJoinRule: " + id;
    }

    //~ Methods ----------------------------------------------------------------

    // override PullUpProjectsAboveJoinRule
    protected boolean hasLeftChild(RelOptRuleCall call)
    {
        return (call.rels.length != 4);
    }

    // override PullUpProjectsAboveJoinRule
    protected boolean hasRightChild(RelOptRuleCall call)
    {
        return call.rels.length > 3;
    }

    // override PullUpProjectsAboveJoinRule
    protected ProjectRel getRightChild(RelOptRuleCall call)
    {
        if (call.rels.length == 4) {
            return (ProjectRel) call.rels[2];
        } else {
            return (ProjectRel) call.rels[3];
        }
    }

    // override PullUpProjectsAboveJoinRule
    protected RelNode getProjectChild(
        RelOptRuleCall call,
        ProjectRel project,
        boolean leftChild)
    {
        // locate the appropriate MultiJoinRel based on which rule was fired
        // and which projection we're dealing with
        MultiJoinRel multiJoin;
        if (leftChild) {
            multiJoin = (MultiJoinRel) call.rels[2];
        } else if (call.rels.length == 4) {
            multiJoin = (MultiJoinRel) call.rels[3];
        } else {
            multiJoin = (MultiJoinRel) call.rels[4];
        }

        // create a new MultiJoinRel that reflects the columns in the projection
        // above the MultiJoinRel
        return RelOptUtil.projectMultiJoin(multiJoin, project);
    }
}

// End PullUpProjectsOnTopOfMultiJoinRule.java

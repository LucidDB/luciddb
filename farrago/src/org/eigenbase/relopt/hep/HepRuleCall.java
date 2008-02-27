/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006-2007 The Eigenbase Project
// Copyright (C) 2006-2007 Disruptive Tech
// Copyright (C) 2006-2007 LucidEra, Inc.
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
package org.eigenbase.relopt.hep;

import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;


/**
 * HepRuleCall implements {@link RelOptRuleCall} for a {@link HepPlanner}. It
 * remembers transformation results so that the planner can choose which one (if
 * any) should replace the original expression.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class HepRuleCall
    extends RelOptRuleCall
{
    //~ Instance fields --------------------------------------------------------

    private List<RelNode> results;

    //~ Constructors -----------------------------------------------------------

    HepRuleCall(
        RelOptPlanner planner,
        RelOptRuleOperand operand,
        RelNode[] rels,
        Map<RelNode, List<RelNode>> nodeChildren)
    {
        super(planner, operand, rels, nodeChildren);

        results = new ArrayList<RelNode>();
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRuleCall
    public void transformTo(RelNode rel)
    {
        RelOptUtil.verifyTypeEquivalence(
            getRels()[0],
            rel,
            getRels()[0]);

        results.add(rel);
    }

    List<RelNode> getResults()
    {
        return results;
    }
}

// End HepRuleCall.java

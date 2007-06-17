/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006-2007 The Eigenbase Project
// Copyright (C) 2002-2007 Disruptive Tech
// Copyright (C) 2006-2007 LucidEra, Inc.
// Portions Copyright (C) 2006-2007 John V. Sichi
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

import org.eigenbase.rel.*;


/**
 * RelTraitPropagationVisitor traverses a RelNode and its <i>unregistered</i>
 * children, making sure that each has a full complement of traits. When a
 * RelNode is found to be missing one or more traits, they are copied from a
 * RelTraitSet given during construction.
 *
 * @author Stephan Zuercher
 */
public class RelTraitPropagationVisitor
    extends RelVisitor
{
    //~ Instance fields --------------------------------------------------------

    private final RelTraitSet baseTraits;
    private final RelOptPlanner planner;

    //~ Constructors -----------------------------------------------------------

    public RelTraitPropagationVisitor(
        RelOptPlanner planner,
        RelTraitSet baseTraits)
    {
        this.planner = planner;
        this.baseTraits = baseTraits;
    }

    //~ Methods ----------------------------------------------------------------

    public void visit(RelNode rel, int ordinal, RelNode parent)
    {
        // REVIEW: SWZ: 1/31/06: We assume that any special RelNodes, such
        // as the VolcanoPlanner's RelSubset always have a full complement
        // of traits and that they either appear as registered or do nothing
        // when childrenAccept is called on them.

        if (planner.isRegistered(rel)) {
            return;
        }

        RelTraitSet relTraits = rel.getTraits();
        for (int i = 0; i < baseTraits.size(); i++) {
            if (i >= relTraits.size()) {
                // Copy traits that the new rel doesn't know about.
                relTraits.addTrait(baseTraits.getTrait(i));
            } else {
                // Verify that the traits are from the same RelTraitDef
                assert relTraits.getTrait(i).getTraitDef()
                    == baseTraits.getTrait(i).getTraitDef();
            }
        }

        rel.childrenAccept(this);
    }
}

// End RelTraitPropagationVisitor.java

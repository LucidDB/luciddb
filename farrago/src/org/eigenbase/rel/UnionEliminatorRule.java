/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later Eigenbase-approved version.
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
package org.eigenbase.rel;

import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelOptRuleOperand;
import org.eigenbase.relopt.RelOptUtil;


/**
 * <code>UnionEliminatorRule</code> checks to see if its possible to
 * optimize a Union call by eliminating the Union operator all together
 * in the case the call consists of only one input.
 *
 * @author Wael Chatila
 * @since Feb 4, 2005
 * @version $Id$
 */
public class UnionEliminatorRule extends RelOptRule
{
    //~ Constructors ----------------------------------------------------------

    public UnionEliminatorRule()
    {
        super(new RelOptRuleOperand(
                UnionRel.class,
                new RelOptRuleOperand [] {
                    new RelOptRuleOperand(RelNode.class, null)
                }));
    }

    //~ Methods ---------------------------------------------------------------

    public void onMatch(RelOptRuleCall call)
    {
        UnionRel union = (UnionRel) call.rels[0];
        if (union.getClass() != UnionRel.class) {
            return;
        }
        if (1 != union.inputs.length) {
            return;
        }
        if (union.isDistinct()) {
            return;
        }
        RelNode child = call.rels[1];
        call.transformTo(RelOptUtil.clone(child));
    }
}

// End UnionEliminatorRule.java

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
package org.eigenbase.rel.rules;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;


/**
 * <code>UnionEliminatorRule</code> checks to see if its possible to optimize a
 * Union call by eliminating the Union operator altogether in the case the call
 * consists of only one input.
 *
 * @author Wael Chatila
 * @version $Id$
 * @since Feb 4, 2005
 */
public class UnionEliminatorRule
    extends RelOptRule
{
    //~ Constructors -----------------------------------------------------------

    public UnionEliminatorRule()
    {
        super(
            new RelOptRuleOperand(
                UnionRel.class,
                ANY));
    }

    //~ Methods ----------------------------------------------------------------

    public void onMatch(RelOptRuleCall call)
    {
        UnionRel union = (UnionRel) call.rels[0];
        if (union.getInputs().length != 1) {
            return;
        }
        if (union.isDistinct()) {
            return;
        }

        // REVIEW jvs 14-Mar-2006:  why don't we need to register
        // the equivalence here like we do in RemoveDistinctRule?
        // And is the clone actually required here?

        RelNode child = union.getInputs()[0];
        call.transformTo(child.clone());
    }
}

// End UnionEliminatorRule.java

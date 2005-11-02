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

import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.RelVisitor;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexShuttle;


/**
 * Walks over a tree of {@link RelNode relational expressions}, walking a
 * {@link RexShuttle} over every expression in that tree.
 *
 * @author jhyde
 * @version $Id$
 *
 * @since 22 October, 2001
 */
public class VisitorRelVisitor extends RelVisitor
{
    //~ Instance fields -------------------------------------------------------

    protected final RexShuttle shuttle;

    //~ Constructors ----------------------------------------------------------

    public VisitorRelVisitor(RexShuttle visitor)
    {
        this.shuttle = visitor;
    }

    //~ Methods ---------------------------------------------------------------

    public void visit(
        RelNode p,
        int ordinal,
        RelNode parent)
    {
        RexNode [] childExps = p.getChildExps();
        for (int i = 0; i < childExps.length; i++) {
            final RexNode exp = childExps[i];
            childExps[i] = exp.accept(shuttle);
        }
        p.childrenAccept(this);
    }
}


// End VisitorRelVisitor.java

/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
// (C) Copyright 2003-2004 John V. Sichi
// You must accept the terms in LICENSE.html to use this software.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

package net.sf.saffron.opt;

import net.sf.saffron.rel.RelVisitor;
import net.sf.saffron.rel.SaffronRel;
import net.sf.saffron.rex.RexNode;
import net.sf.saffron.rex.RexShuttle;

/**
 * Walks over a tree of {@link SaffronRel relational expressions}, walking a
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

    public void visit(SaffronRel p,int ordinal,SaffronRel parent)
    {
        RexNode [] childExps = p.getChildExps();
        for (int i = 0; i < childExps.length; i++) {
            final RexNode exp = childExps[i];
            childExps[i] = shuttle.visit(exp);
        }
        p.childrenAccept(this);
    }
}


// End VisitorRelVisitor.java

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

package net.sf.saffron.rel;

/**
 * A <code>RelVisitor</code> is a Visitor role in the
 * {@link net.sf.saffron.util.Glossary#VisitorPattern visitor pattern} and
 * visits {@link SaffronRel} objects as the role of Element.  Other
 * components in the pattern: {@link SaffronRel#childrenAccept(RelVisitor)}.
 *
 * @author jhyde
 * @version $Id$
 *
 * @since 22 September, 2001
 */
public abstract class RelVisitor
{
    //~ Methods ---------------------------------------------------------------

    public void visit(SaffronRel p,int ordinal,SaffronRel parent)
    {
        p.childrenAccept(this);
    }
}


// End RelVisitor.java

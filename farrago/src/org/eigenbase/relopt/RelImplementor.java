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

package org.eigenbase.relopt;

import org.eigenbase.rel.RelNode;

/**
 * Callback used to hold state while converting a tree of
 * {@link RelNode relational expressions} into a plan. Calling conventions
 * typically have their own protocol for walking over a tree, and
 * correspondingly have their own implementors, which are subclasses of
 * <code>RelImplementor</code>.
 */
public interface RelImplementor
{
    /**
     * Implements a relational expression according to a calling convention.
     */
    Object visitChild(RelNode parent, int ordinal, RelNode child);

    /**
     * Called from {@link #visitChild} after the frame has been set up.
     * Specific implementors should override this method.
     * @param child Child relational expression
     * @return
     */
    Object visitChildInternal(RelNode child);
}


// End RelImplementor.java

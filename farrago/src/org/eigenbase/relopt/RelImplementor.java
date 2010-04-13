/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2002 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 2003 John V. Sichi
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
 * Callback used to hold state while converting a tree of {@link RelNode
 * relational expressions} into a plan. Calling conventions typically have their
 * own protocol for walking over a tree, and correspondingly have their own
 * implementors, which are subclasses of <code>RelImplementor</code>.
 *
 * @version $Id$
 */
public interface RelImplementor
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Implements a relational expression according to a calling convention.
     *
     * @param parent Parent relational expression
     * @param ordinal Ordinal of child within its parent
     * @param child Child relational expression
     *
     * @return Interpretation of the return value is left to the implementor
     */
    Object visitChild(
        RelNode parent,
        int ordinal,
        RelNode child);

    /**
     * Called from {@link #visitChild} after the frame has been set up. Specific
     * implementors should override this method.
     *
     * @param child Child relational expression
     *
     * @return Interpretation of the return value is left to the implementor
     */
    Object visitChildInternal(RelNode child);

    /**
     * Called from {@link #visitChild} after the frame has been set up. Specific
     * implementors should override this method.
     *
     * @param child Child relational expression
     * @param ordinal Ordinal of child within its parent
     *
     * @return Interpretation of the return value is left to the implementor
     */
    Object visitChildInternal(RelNode child, int ordinal);
}

// End RelImplementor.java

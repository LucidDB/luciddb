/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006-2006 The Eigenbase Project
// Copyright (C) 2006-2006 Disruptive Tech
// Copyright (C) 2006-2006 LucidEra, Inc.
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

/**
 * HepMatchOrder specifies the order of graph traversal when looking for rule
 * matches.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public enum HepMatchOrder
{
    /**
     * Match in arbitrary order.  This is the default because it is the most
     * efficient, and most rules don't care about order.
     */
    ARBITRARY,

    /**
     * Match from leaves up.  A match attempt at a descendant precedes
     * all match attempts at its ancestors.
     */
    BOTTOM_UP,

    /**
     * Match from root down.  A match attempt at an ancestor always
     * precedes all match attempts at its descendants.
     */
    TOP_DOWN
}

// End HepMatchOrder.java

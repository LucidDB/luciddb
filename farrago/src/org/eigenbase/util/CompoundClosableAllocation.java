/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2003-2007 John V. Sichi
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
package org.eigenbase.util;

import java.util.*;


/**
 * CompoundClosableAllocation represents a collection of ClosableAllocations
 * which share a common lifecycle. It guarantees that allocations are closed in
 * the reverse order in which they were added.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class CompoundClosableAllocation
    implements ClosableAllocationOwner
{
    //~ Instance fields --------------------------------------------------------

    /**
     * List of owned ClosableAllocation objects.
     */
    protected List<ClosableAllocation> allocations;

    //~ Constructors -----------------------------------------------------------

    public CompoundClosableAllocation()
    {
        allocations = new LinkedList<ClosableAllocation>();
    }

    //~ Methods ----------------------------------------------------------------

    // implement ClosableAllocationOwner
    public void addAllocation(ClosableAllocation allocation)
    {
        allocations.add(allocation);
    }

    // implement ClosableAllocation
    public void closeAllocation()
    {
        //        try {
        // traverse in reverse order
        ListIterator<ClosableAllocation> iter =
            allocations.listIterator(allocations.size());
        while (iter.hasPrevious()) {
            ClosableAllocation allocation = iter.previous();

            // NOTE:  nullify the entry just retrieved so that if allocation
            // calls back to forgetAllocation, it won't find itself
            // (this prevents a ConcurrentModificationException)
            iter.set(null);
            allocation.closeAllocation();
        }
        allocations.clear();
        // } catch (ConcurrentModificationException e) { throw
        // Util.newInternal(e, "in " + getClass()); }
    }

    /**
     * Forgets an allocation without closing it.
     *
     * @param allocation the allocation to forget
     *
     * @return whether the allocation was known
     */
    public boolean forgetAllocation(ClosableAllocation allocation)
    {
        return allocations.remove(allocation);
    }
}

// End CompoundClosableAllocation.java

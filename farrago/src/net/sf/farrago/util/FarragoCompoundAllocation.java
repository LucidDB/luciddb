/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
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
package net.sf.farrago.util;

import java.util.*;


/**
 * FarragoCompoundAllocation represents a collection of FarragoAllocations
 * which share a common lifecycle.  It guarantees that allocations are closed
 * in the reverse order in which they were added.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoCompoundAllocation implements FarragoAllocationOwner
{
    //~ Instance fields -------------------------------------------------------

    /**
     * List of owned FarragoAllocation objects.
     */
    protected List allocations;

    //~ Constructors ----------------------------------------------------------

    public FarragoCompoundAllocation()
    {
        allocations = new LinkedList();
    }

    //~ Methods ---------------------------------------------------------------

    // implement FarragoAllocationOwner
    public void addAllocation(FarragoAllocation allocation)
    {
        allocations.add(allocation);
    }

    // implement FarragoAllocation
    public void closeAllocation()
    {
        // traverse in reverse order
        ListIterator iter = allocations.listIterator(allocations.size());
        while (iter.hasPrevious()) {
            FarragoAllocation allocation = (FarragoAllocation) iter.previous();

            // NOTE:  nullify the entry just retrieved so that if allocation
            // calls back to forgetAllocation, it won't find itself
            // (this prevents a ConcurrentModificationException)
            iter.set(null);
            allocation.closeAllocation();
        }
        allocations.clear();
    }

    /**
     * Forgets an allocation without closing it.
     *
     * @param allocation the allocation to forget
     *
     * @return whether the allocation was known
     */
    public boolean forgetAllocation(FarragoAllocation allocation)
    {
        return allocations.remove(allocation);
    }
}


// End FarragoCompoundAllocation.java

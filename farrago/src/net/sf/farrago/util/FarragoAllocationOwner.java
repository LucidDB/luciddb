/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
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
package net.sf.farrago.util;


/**
 * FarragoAllocationOwner represents an object which can take ownership of
 * FarragoAllocations and guarantee that they will be cleaned up correctly
 * when its own closeAllocation() is called.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface FarragoAllocationOwner extends FarragoAllocation
{
    //~ Methods ---------------------------------------------------------------

    /**
     * Assigns ownership of a FarragoAllocation to this owner.
     *
     * @param allocation the FarragoAllocation to take over
     */
    public void addAllocation(FarragoAllocation allocation);
}


// End FarragoAllocationOwner.java

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
 * FarragoCacheEntry implements the interfaces for a cache entry.
 *
 * @version $Id$
 */
public class FarragoCacheEntry
    implements FarragoObjectCache.Entry, FarragoObjectCache.UninitializedEntry
{
    //~ Static fields/initializers --------------------------------------------

    // NOTE jvs 15-July-2004: entry attribute synchronization is
    // fine-grained; pinCount is protected by FarragoObjectCache.mapKeyToEntry's
    // monitor, while the others are protected by the entry's monitor.
    Object key;
    Object value;
    int pinCount;
    long memoryUsage;
    Thread constructionThread;

    /**
     * The cache this entry is associated with
     */
    FarragoObjectCache parentCache;

    //~ Constructor -----------------------------------------------------------

    public FarragoCacheEntry(FarragoObjectCache parentCache)
    {
        this.parentCache = parentCache;
    }

    //~ Methods ----------------------------------------------------------------

    // implement Entry
    public Object getKey()
    {
        return key;
    }

    // implement Entry
    public Object getValue()
    {
        return value;
    }

    // implement UninitializedEntry
    public void initialize(
        Object value,
        long memoryUsage)
    {
        this.value = value;
        this.memoryUsage = memoryUsage;
    }

    // implement FarragoAllocation
    public void closeAllocation()
    {
        parentCache.unpin(this);
    }
}

// End FarragoCacheEntry.java

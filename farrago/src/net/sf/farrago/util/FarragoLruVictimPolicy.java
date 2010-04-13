/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
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
package net.sf.farrago.util;

import java.util.*;


/**
 * FarragoLruVictimPolicy implements an LRU caching policy for the
 * FarragoObjectCache.
 *
 * <p>This class assumes that synchronization is handled by its caller.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class FarragoLruVictimPolicy
    implements FarragoCacheVictimPolicy
{
    //~ Instance fields --------------------------------------------------------

    /**
     * LRU ordering of objects in the cache. The linked list provides the LRU
     * ordering.
     */
    private final LinkedHashSet<FarragoCacheEntry> lruCacheOrder;

    //~ Constructors -----------------------------------------------------------

    public FarragoLruVictimPolicy()
    {
        lruCacheOrder = new LinkedHashSet<FarragoCacheEntry>();
    }

    //~ Methods ----------------------------------------------------------------

    // implement FarragoCacheVictimPolicy
    public FarragoCacheEntry newEntry(FarragoObjectCache parentCache)
    {
        return new FarragoCacheEntry(parentCache);
    }

    // implement FarragoCacheVictimPolicy
    public void registerEntry(FarragoCacheEntry entry)
    {
        lruCacheOrder.add(entry);
    }

    // implement FarragoCacheVictimPolicy
    public void unregisterEntry(Iterator victimRange)
    {
        victimRange.remove();
    }

    // implement FarragoCacheVictimPolicy
    public void unregisterEntry(FarragoCacheEntry entry)
    {
        boolean rc = lruCacheOrder.remove(entry);
        assert (rc);
    }

    // implement FarragoCacheVictimPolicy
    public void accessEntry(FarragoCacheEntry entry)
    {
        // remove the object from the list and add it to the end of the list
        boolean rc = lruCacheOrder.remove(entry);
        assert (rc);
        lruCacheOrder.add(entry);
    }

    // implement FarragoCacheVictimPolicy
    public Iterator<FarragoCacheEntry> getVictimIterator()
    {
        return lruCacheOrder.iterator();
    }

    // implement FarragoCacheVictimPolicy
    public void clearCache()
    {
        lruCacheOrder.clear();
    }
}

// End FarragoLruVictimPolicy.java

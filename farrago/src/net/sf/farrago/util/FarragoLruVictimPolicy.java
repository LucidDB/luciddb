/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
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

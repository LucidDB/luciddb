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
 * FarragoCacheVictimPolicy defines the interface for different implementations
 * of a cache victimization policy for the FarragoObjectCache.
 * FarragoObjectCache calls the appropriate notification methods when entries
 * are added, removed, or accessed from the cache, allowing the policy to
 * determine the order in which entries should be victimized from the cache,
 * when the cache becomes full.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public interface FarragoCacheVictimPolicy
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Creates a new cache entry
     *
     * @param parentCache the cache this entry is associated with
     */
    public FarragoCacheEntry newEntry(FarragoObjectCache parentCache);

    /**
     * Receives notification that a new entry is being added to the cache.
     *
     * @param entry new entry being added to the cache
     */
    public void registerEntry(FarragoCacheEntry entry);

    /**
     * Receives notification that an entry is being removed from the cache.
     *
     * @param entry entry to be removed
     */
    public void unregisterEntry(FarragoCacheEntry entry);

    /**
     * Unregisters the current entry being accessed from the victim range
     *
     * @param victimRange iterator corresponding to the victim range
     */
    public void unregisterEntry(Iterator victimRange);

    /**
     * Receives notification that an existing entry in the cache is being
     * accessed.
     *
     * @param entry entry being accessed
     */
    public void accessEntry(FarragoCacheEntry entry);

    /**
     * Returns an iterator corresponding to a list of potential entries for
     * removal from the cache, in the order of precedence.
     *
     * @return iterator corresponding to cache victims
     */
    public Iterator<FarragoCacheEntry> getVictimIterator();

    /**
     * Receives notification that all entries are being removed from the cache
     */
    public void clearCache();
}

// End FarragoCacheVictimPolicy.java

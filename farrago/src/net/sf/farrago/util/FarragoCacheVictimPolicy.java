/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 SQLstream, Inc.
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

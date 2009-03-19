/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 2003-2009 John V. Sichi
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

import java.util.concurrent.atomic.*;


/**
 * FarragoCacheEntry implements the interfaces for a cache entry.
 *
 * @version $Id$
 */
public class FarragoCacheEntry
    implements FarragoObjectCache.Entry,
        FarragoObjectCache.UninitializedEntry
{
    //~ Instance fields --------------------------------------------------------

    // NOTE jvs 15-July-2004: entry attribute synchronization is fine-grained;
    // pinCount is protected by FarragoObjectCache.mapKeyToEntry's monitor,
    // while the others are protected by the entry's monitor.
    Object key;
    Object value;
    int pinCount;
    AtomicLong memoryUsage;
    Thread constructionThread;
    boolean isReusable;
    boolean isInitialized;

    /**
     * The cache this entry is associated with
     */
    FarragoObjectCache parentCache;

    //~ Constructors -----------------------------------------------------------

    public FarragoCacheEntry(FarragoObjectCache parentCache)
    {
        this.parentCache = parentCache;

        // assume reusable; but really, assertions below should guarantee that
        // this is never even accessed until after initialize overwrites it
        isReusable = true;
        memoryUsage = new AtomicLong();
    }

    //~ Methods ----------------------------------------------------------------

    // implement Entry
    public Object getKey()
    {
        // NOTE jvs 14-Jun-2007:  Don't assert isInitialized, since
        // an entry gets its key set before initialization.
        return key;
    }

    // implement Entry
    public Object getValue()
    {
        assert (isInitialized);
        return value;
    }

    // implement Entry
    public boolean isReusable()
    {
        assert (isInitialized);
        return isReusable;
    }

    // implement UninitializedEntry
    public void initialize(
        Object value,
        long memoryUsage,
        boolean isReusable)
    {
        // REVIEW jvs 15-Jun-2007: Order of initialization is important here
        // due to access by unsynchronized code in FarragoObjectCache.  I'm not
        // sure if that's safe on all architectures--could the lack of a read
        // memory barrier cause the writes to get reordered?
        this.isInitialized = true;
        this.isReusable = isReusable;
        this.memoryUsage.set(memoryUsage);
        this.value = value;
    }

    // implement FarragoAllocation
    public void closeAllocation()
    {
        assert (isInitialized);
        parentCache.unpin(this);
    }

    public String toString()
    {
        return "FarragoCacheEntry: key=" + key + ", value=" + value
            + ", pinCount=" + pinCount;
    }

    /**
     * @return whether {@link #initialize} has been called yet
     */
    boolean isInitialized()
    {
        return isInitialized;
    }
}

// End FarragoCacheEntry.java

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

import java.util.*;
import java.util.logging.*;

import net.sf.farrago.trace.*;

import org.eigenbase.util.*;


/**
 * FarragoObjectCache implements generic object caching. It doesn't use
 * SoftReferences since from what I've read, typical JVM implementations don't
 * give good results.
 *
 * <p>Key objects must implement hashCode/equals properly since
 * FarragoObjectCache is based on a HashMap internally.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoObjectCache
    implements FarragoAllocation
{

    //~ Static fields/initializers ---------------------------------------------

    private static final Logger tracer = FarragoTrace.getObjectCacheTracer();

    //~ Instance fields --------------------------------------------------------

    /**
     * Map from cache key to EntryImpl. To avoid deadlock, synchronization order
     * is always map first, entry second.
     */
    private MultiMap<Object, FarragoCacheEntry> mapKeyToEntry;
    private long bytesMax;

    /**
     * Number of bytes currently in use by cached objects. This and bytesMax are
     * synchronized via mapKeyToEntry monitor.
     */
    private long bytesUsed;

    /**
     * Victimization policy for this cache
     */
    private FarragoCacheVictimPolicy victimPolicy;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates an empty cache.
     *
     * @param owner FarragoAllocationOwner for this cache, to make sure
     * everything gets discarded eventually
     * @param bytesMax maximum number of bytes to cache
     * @param victimPolicy victimization policy to use when the cache is full
     */
    public FarragoObjectCache(
        FarragoAllocationOwner owner,
        long bytesMax,
        FarragoCacheVictimPolicy victimPolicy)
    {
        mapKeyToEntry = new MultiMap<Object, FarragoCacheEntry>();
        owner.addAllocation(this);
        this.bytesMax = bytesMax;
        bytesUsed = 0;
        this.victimPolicy = victimPolicy;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Pins an entry in the cache. When the caller is done with it, the returned
     * entry must be unpinned, otherwise the entry can never be discarded from
     * the cache.
     *
     * @param key key of the entry to pin
     * @param factory CachedObjectFactory to call if an existing entry can't be
     * used, in which case a new entry will be created and initialized by
     * calling the factory's initializeEntry method
     * @param exclusive if true, only reuse unpinned entries
     *
     * @return pinned Entry
     */
    public Entry pin(
        Object key,
        CachedObjectFactory factory,
        boolean exclusive)
    {
        if (tracer.isLoggable(Level.FINE)) {
            tracer.fine("Pinning key " + key.toString());
        }

        Thread currentThread = Thread.currentThread();

        // look up entry in map
        FarragoCacheEntry entry = null;

        synchronized (mapKeyToEntry) {
            for (FarragoCacheEntry entry1 : mapKeyToEntry.getMulti(key)) {
                entry = entry1;
                if (exclusive && (entry.pinCount != 0)) {
                    // this one's already in use by someone else
                    entry = null;
                } else {
                    tracer.finer("found cache entry");

                    // pin the entry so that it can't be discarded after map
                    // lock is released below
                    entry.pinCount++;
                    victimPolicy.accessEntry(entry);
                    break;
                }
            }
            if (entry == null) {
                // create a new entry and add it to the map
                entry = victimPolicy.newEntry(this);
                entry.key = key;
                entry.pinCount = 1;
                victimPolicy.registerEntry(entry);

                // let others know we're planning to construct it, so they
                // shouldn't
                entry.constructionThread = currentThread;
                mapKeyToEntry.putMulti(key, entry);
            }
        }

        // TODO jvs 15-July-2004:  break up this oversized method release map
        // lock since construction work below may be time-consuming
        boolean unpinEntry = false;
        try {
            synchronized (entry) {
                for (;;) {
                    if (entry.constructionThread == currentThread) {
                        // we're responsible for construction
                        boolean success = false;
                        try {
                            factory.initializeEntry(key, entry);
                            success = true;
                            tracer.finer("initialized new cache entry");
                        } finally {
                            // NOTE: an exception can leave a failed entry lying
                            // around.  It would be nice to get rid of it
                            // immediately, but it's tricky since someone else
                            // may already be waiting for it.
                            if (!success) {
                                tracer.finer("entry initialization failed");

                                // if unsuccessful, we're unwinding, so don't
                                // leave failed entry pinned; can't unpin
                                // here since we still hold entry lock
                                unpinEntry = true;
                            }

                            // let others know that our attempt is complete
                            // (though not necessarily successful)
                            entry.constructionThread = null;
                            entry.notifyAll();
                        }

                        // now we need to adjust memory usage, but can only do
                        // that after releasing entry lock since map lock is
                        // required
                        break;
                    }

                    while (entry.constructionThread != null) {
                        tracer.finer("waiting for entry initialization");

                        // someone else is supposed to construct the object
                        try {
                            entry.wait();
                        } catch (InterruptedException ex) {
                            throw new AssertionError();
                        }
                    }

                    if (entry.value != null) {
                        if (tracer.isLoggable(Level.FINE)) {
                            tracer.fine(
                                "returning entry with pin count = "
                                + entry.pinCount);
                        }

                        // got it
                        return entry;
                    }

                    // Someone else's attempt must have failed; we'll give it a
                    // shot ourselves.  Most likely we'll fail too, but doing it
                    // this way is easier than trying to replicate the original
                    // exception.
                    entry.constructionThread = currentThread;
                }
            }
        } finally {
            if (unpinEntry) {
                synchronized (mapKeyToEntry) {
                    entry.pinCount--;
                }
            }
        }

        // REVIEW mberkowitz 1-Jul-2006: when (unpinEntry) this seems to
        // return an unpinned entry and to account for its memory.
        if (tracer.isLoggable(Level.FINE)) {
            long cacheSize = bytesUsed + entry.memoryUsage;
            tracer.fine(
                "returning new entry, pin count " + entry.pinCount
                + ", size " + entry.memoryUsage + ", cache size " + cacheSize
                + ", key " + entry.key);
        }
        adjustMemoryUsage(entry.memoryUsage);
        return entry;
    }

    private void adjustMemoryUsage(long incBytes)
    {
        List<FarragoCacheEntry> discards;

        synchronized (mapKeyToEntry) {
            bytesUsed += incBytes;

            long overdraft = bytesUsed - bytesMax;

            if (overdraft <= 0) {
                return;
            }

            discards = new ArrayList<FarragoCacheEntry>();

            // get an ordered list of potential cache victims and search
            // for unused entries
            Iterator<FarragoCacheEntry> lruList = victimPolicy.getVictimIterator();
            while ((overdraft > 0) && lruList.hasNext()) {
                FarragoCacheEntry entry = lruList.next();
                if (entry.pinCount > 0) {
                    continue;
                }
                victimPolicy.unregisterEntry(lruList);
                mapKeyToEntry.removeMulti(entry.getKey(), entry);
                discards.add(entry);
                overdraft -= entry.memoryUsage;
            }
        }

        // release map lock since actual discard could be time-consuming
        for (FarragoCacheEntry discard : discards) {
            discardEntry(discard);
        }
        if (tracer.isLoggable(Level.FINE)) {
            tracer.finer("cache size after discards = " + bytesUsed);
        }

        // REVIEW:  in some circumstances, we want to fail if overdraft is
        // still positive
    }

    /**
     * Changes the cache size limit, discarding entries if necessary.
     *
     * @param bytesMaxNew new limit
     */
    public void setMaxBytes(long bytesMaxNew)
    {
        synchronized (mapKeyToEntry) {
            bytesMax = bytesMaxNew;
        }

        adjustMemoryUsage(0);
    }

    /**
     * @return cache size limit
     */
    public long getBytesMax()
    {
        return bytesMax;
    }

    /**
     * Unpins an entry returned by pin. After unpin, the caller should
     * immediately nullify its reference to the entry, its key, its value, and
     * any sub-objects so that they can be garbage collected.
     *
     * @param pinnedEntry pinned Entry
     */
    public void unpin(Entry pinnedEntry)
    {
        FarragoCacheEntry entry = (FarragoCacheEntry) pinnedEntry;
        synchronized (mapKeyToEntry) {
            if (tracer.isLoggable(Level.FINE)) {
                tracer.fine("Unpinning key " + entry.key.toString());
                tracer.fine("pin count before unpin = " + entry.pinCount);
            }
            assert (entry.pinCount > 0);
            entry.pinCount--;
        }

        // in case too much was pinned
        adjustMemoryUsage(0);
    }

    /**
     * Removes an entry from the cache, but does not close its value. The entry
     * must be exclusive: pinned only by the caller.
     *
     * @return the former value of the entry.
     */
    public Object detach(Entry e)
    {
        FarragoCacheEntry entry = (FarragoCacheEntry) e;
        Object val = entry.value;
        synchronized (mapKeyToEntry) {
            synchronized (e) {
                if (tracer.isLoggable(Level.FINE)) {
                    tracer.fine(
                        "Detaching entry " + entry.key.toString()
                        + ", size " + entry.memoryUsage);
                }
                assert (entry.pinCount == 1) : entry;
                mapKeyToEntry.removeMulti(
                    entry.getKey(),
                    entry);
                victimPolicy.unregisterEntry(entry);
                bytesUsed -= entry.memoryUsage;
                if (tracer.isLoggable(Level.FINER)) {
                    tracer.finer("cache size now " + bytesUsed);
                }
                entry.value = null;
            }
        }
        return val;
    }

    /**
     * Discards any entries associated with a key. If the bound value of an
     * entry is a ClosableObject, it will be closed.
     *
     * @param key key of the Entry to discard
     */
    public void discard(Object key)
    {
        List<FarragoCacheEntry> list;
        synchronized (mapKeyToEntry) {
            list = mapKeyToEntry.getMulti(key);
            mapKeyToEntry.remove(key);
            for (FarragoCacheEntry entry : list) {
                victimPolicy.unregisterEntry(entry);
            }
        }
        // release the synch lock before discarding the actual entries,
        // as that may take a while
        for (FarragoCacheEntry entry : list) {
            discardEntry(entry);
        }
    }

    /**
     * Discards all entries.
     */
    public void discardAll()
    {
        tracer.fine("discarding all entries");
        synchronized (mapKeyToEntry) {
            Iterator<Map.Entry<Object, FarragoCacheEntry>> iter =
                mapKeyToEntry.entryIterMulti();
            while (iter.hasNext()) {
                Map.Entry<Object, FarragoCacheEntry> mapEntry = iter.next();
                FarragoCacheEntry entry = mapEntry.getValue();
                discardEntry(entry);
            }
            mapKeyToEntry.clear();
            victimPolicy.clearCache();
        }
    }

    private void discardEntry(FarragoCacheEntry entry)
    {
        synchronized (entry) {
            if (tracer.isLoggable(Level.FINE)) {
                tracer.fine(
                    "Discarding entry " + entry.key.toString()
                    + ", size " + entry.memoryUsage);
            }

            assert (entry.pinCount == 0) :
                "expected pin-count=0 for entry " + entry;
            assert (entry.constructionThread == null) : entry;

            if (entry.value instanceof FarragoAllocation) {
                ((FarragoAllocation) entry.value).closeAllocation();
            }
        }

        synchronized (mapKeyToEntry) {
            bytesUsed -= entry.memoryUsage;
        }
    }

    // implement FarragoAllocation
    public void closeAllocation()
    {
        discardAll();
        assert (bytesUsed == 0);
    }

    //~ Inner Interfaces -------------------------------------------------------

    /**
     * Factory interface for producing cached objects. This must be implemented
     * by callers to FarragoObjectCache.
     */
    public static interface CachedObjectFactory
    {
        /**
         * Initialize a cache entry.
         *
         * @param key key of the object to be constructed
         * @param entry to initialize by calling its initialize() method
         */
        public void initializeEntry(
            Object key,
            UninitializedEntry entry);
    }

    /**
     * Callback interface for entry initialization.
     */
    public interface UninitializedEntry
    {
        /**
         * Initializes the entry.
         *
         * @param value the value to associate with the entry's key; if this
         * Object implements FarragoAllocation, it will be closed when discarded
         * from the cache
         * @param memoryUsage approximate total number of bytes of memory used
         * by entry (combination of key, value, and any sub-objects)
         */
        public void initialize(
            Object value,
            long memoryUsage);
    }

    /**
     * Interface for a cache entry; same as Map.Entry except that there is no
     * requirement on equals/hashCode. This is implemented by
     * FarragoObjectCache.
     *
     * <p>Entry extends FarragoAllocation; its closeAllocation implementation
     * calls unpin.
     */
    public interface Entry
        extends FarragoAllocation
    {
        public Object getKey();

        public Object getValue();
    }
}

// End FarragoObjectCache.java

/*
// $Id$
// Farrago is an extensible data management system.
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
package net.sf.farrago.util;

import java.util.*;
import java.util.logging.*;

import net.sf.farrago.trace.*;

import org.eigenbase.util.*;


/**
 * FarragoObjectCache implements generic object caching. It doesn't use
 * SoftReferences since those don't provide enough programmatic control over
 * memory-sensitive caching policies.
 *
 * <p>Key objects must implement hashCode/equals properly since
 * FarragoObjectCache is based on a HashMap internally.
 *
 * <p>See {@link net.sf.farrago.test.FarragoObjectCacheTest} for examples
 * of usage patterns.
 *
 * <p>Note that {@link #closeAllocation} should only be called with
 * no entries pinned, no calls in progress, and no further calls planned.
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
     * Map from cache key to EntryImpl. To avoid deadlock, synchronize
     * on either map or entry but not both at once.  See code comments in
     * tryPin for more info on this.
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
     *<p>
     *
     * Note that for a cache miss with exclusive=false, other callers
     * requesting to pin the same key near-simultaneously will wait for the
     * initialization of the new object to complete.  If it completes
     * successfully, AND it is initialized as reusable, then the new object
     * will be shared.  If it completes unsuccessfully, the first caller will
     * receive the thrown exception, and subsequent callers will retry the
     * attempt themselves.  If it completes successfully, but turns out to be
     * non-reusable, then subsequent callers will give up on it and create
     * their own private copies instead.
     *
     * @param key key of the entry to pin
     * @param factory CachedObjectFactory to call if an existing entry can't be
     * used (cache miss), in which case a new entry will be created and
     * initialized by calling the factory's initializeEntry method
     * @param exclusive if true, only reuse unpinned entries; note that
     * this flag is not remembered along with the entry, so callers
     * must be consistent in setting this flag for objects in the same
     * keyspace
     *
     * @return pinned entry
     */
    public Entry pin(
        Object key,
        CachedObjectFactory factory,
        boolean exclusive)
    {
        if (tracer.isLoggable(Level.FINE)) {
            tracer.fine("Pinning key " + key.toString());
        }

        // NOTE jvs 14-Jun-2007: Although it may appear that this loop burns
        // 100% CPU, that's not the case.  There's a wait inside of tryPin; if
        // a null entry is returned because the entry being waited for turned
        // out not to be usable by us, then we retry from the top.  This loop
        // implements that retry logic.  A pathological access pattern could
        // lead to starvation.
        for (;;) {
            Entry entry = tryPin(key, factory, exclusive);
            if (entry != null) {
                return entry;
            }
            if (tracer.isLoggable(Level.FINE)) {
                tracer.fine("Retrying pin attempt for key " + key.toString());
            }
        }
    }
    
    private Entry tryPin(
        Object key,
        CachedObjectFactory factory,
        boolean exclusive)
    {
        Thread currentThread = Thread.currentThread();

        // Look up entry in map, or create a new one.  Either way, it comes
        // back pinned.  Note that we both acquire and release map lock in here
        // since construction work below may be time-consuming.
        FarragoCacheEntry entry =
            findOrCreateEntry(currentThread, key, factory, exclusive);
        
        boolean unpinEntry = false;
        try {
            synchronized (entry) {
                for (;;) {
                    if (entry.constructionThread == currentThread) {
                        // we're responsible for construction
                        boolean success = false;
                        try {
                            // NOTE jvs 14-Jun-2007: An important
                            // synchronization issue here is that we don't know
                            // what initializeEntry is going to do; in fact, it
                            // is allowed to call back into pin or unpin in
                            // order to build a top-level cached object
                            // composed of several underlying cached objects.
                            // This means that we may end up with the
                            // lock sequence entry-then-map.  That's why
                            // we don't allow locking of entries when a
                            // lock on the map is held.
                            factory.initializeEntry(key, entry);
                            assert(entry.isInitialized());
                            // TODO jvs 10-Jun-2007:  assert that
                            // new value is not stale-on-arrival?  Maybe
                            // only when trace is on?
                            success = true;
                            tracer.finer("initialized new cache entry");
                        } finally {
                            // NOTE: an exception can leave a failed entry lying
                            // around.  It would be nice to get rid of it
                            // immediately, but it's tricky since someone else
                            // may already be waiting for it.
                            if (!success) {
                                tracer.finer("entry initialization failed");

                                // If unsuccessful, we're unwinding, so don't
                                // leave failed entry pinned; can't unpin here
                                // since we still hold entry lock.  This means
                                // we'll actually leave a garbage entry lying
                                // around in the cache, but that's OK; pin
                                // requests know how to avoid it, and
                                // eventually it will be discarded.
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
                        if (!entry.isReusable()) {
                            // Oops, we were waiting for something that
                            // turned out not to be reusable.  We'll
                            // have to retry from the top since the original
                            // entry has already been returned as private
                            // to the construction-initiating caller.
                            unpinEntry = true;
                            return null;
                        }
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

        // NOTE jvs 10-Jun-2007: We never get here with unpinEntry=true,
        // because the only path above which sets it is one in which an
        // exception gets thrown and not caught.  One related minor issue is
        // that in the failed-initialization case, we don't bump up the memory
        // usage at all, even though we leave behind the dead entry (with
        // value=null) in the cache.  It's not large, and will get aged
        // out eventually.
        assert(!unpinEntry);

        if (tracer.isLoggable(Level.FINE)) {
            long cacheSize = bytesUsed + entry.memoryUsage;
            tracer.fine(
                "returning new entry, pin count " + entry.pinCount
                + ", size " + entry.memoryUsage + ", cache size "
                + cacheSize + ", key " + entry.key);
        }
        adjustMemoryUsage(entry.memoryUsage);
        return entry;
    }

    private FarragoCacheEntry findOrCreateEntry(
        Thread currentThread,
        Object key,
        CachedObjectFactory factory,
        boolean exclusive)
    {
        FarragoCacheEntry entry = null;

        List<FarragoCacheEntry> staleList = null;

        synchronized (mapKeyToEntry) {
            List<FarragoCacheEntry> candidateList = mapKeyToEntry.getMulti(key);
            Iterator<FarragoCacheEntry> iter = candidateList.iterator();
            while (iter.hasNext()) {
                entry = iter.next();
                if (exclusive && (entry.pinCount != 0)) {
                    // this one's already in use by someone else
                    entry = null;
                } else {
                    // NOTE jvs 15-Jun-2007:  We can't synchronize on
                    // entry here, so have to be careful in how we access it.
                    Object value = entry.value;
                    // REVIEW jvs 14-Jun-2007: If value is null, and pin-count
                    // is 0, perhaps we could be a good citizen and discard it
                    // as garbage in passing.
                    if (value != null) {
                        if (!entry.isReusable() || factory.isStale(value)) {
                            if (entry.pinCount == 0) {
                                tracer.finer(
                                    "found stale+unpinned cache entry:  "
                                    + "adding to discard list");
                                if (staleList == null) {
                                    staleList =
                                        new ArrayList<FarragoCacheEntry>();
                                }
                                staleList.add(entry);
                                if (candidateList.size() > 1) {
                                    // NOTE jvs 10-Jun-2007: See comment with
                                    // same date below for the reason behind
                                    // this special case.
                                    iter.remove();
                                }
                                victimPolicy.unregisterEntry(entry);
                            } else {
                                tracer.finer(
                                    "found stale+pinned cache entry:  "
                                    + "ignoring");
                            }
                            entry = null;
                            continue;
                        }
                    }

                    tracer.finer("found cache entry");

                    // pin the entry so that it can't be discarded after map
                    // lock is released below
                    entry.pinCount++;
                    victimPolicy.accessEntry(entry);
                    break;
                }
            }
            if ((staleList != null) && (candidateList.size() == 1)) {
                // NOTE jvs 10-Jun-2007: This special case is required because
                // of the non-uniform return behavior of MultiMap (singleton
                // entries are returned via an immutable list).
                mapKeyToEntry.remove(key);
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

        if (staleList != null) {
            // Put out the garbage.  We deferred this above due to
            // synchronization requirements.
            for (FarragoCacheEntry discard : staleList) {
                discardEntry(discard);
            }
        }

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
            Iterator<FarragoCacheEntry> lruList =
                victimPolicy.getVictimIterator();
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
     * @return current number of bytes cached (regardless of whether
     * they are pinned or not)
     */
    public long getBytesCached()
    {
        return bytesUsed;
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
     * Discards any entries associated with a key. If the bound value of an
     * entry is a ClosableObject, it will be closed.
     *
     *<p>
     *
     * REVIEW jvs 10-Jun-2007: This method is unsafe since the associated
     * entries may still be pinned.  Code which is relying on it (such as
     * FarragoDataWrapperCache) should probably be changed to use either a
     * staleness test or a different key scheme.
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
     * Discards all entries.  May only be called at a time when
     * no entries are currently pinned (otherwise assertion failures
     * may result).
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

            assert (entry.pinCount == 0) : "expected pin-count=0 for entry "
                + entry;
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
         * Initializes a cache entry.
         *
         * @param key key of the object to be constructed
         * @param entry to initialize by calling its {@link
         * UninitializedEntry#initialize} method; failing to call initialize
         * will lead to a subsequent assertion (unless an exception
         * is thrown to indicate initialization failure)
         */
        public void initializeEntry(
            Object key,
            UninitializedEntry entry);

        /**
         * Tests a cached object for staleness.
         *
         * @return true if object is stale, meaning it must not
         * be returned from a pin call, and should be discarded from
         * the cache when detected
         */
        public boolean isStale(Object value);
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
         * @param isReusable whether the initialized entry is
         * reusable; if false, the entry will be returned as private
         * to the original caller of {@link #pin}, and no other callers will
         * ever be able to pin it
         */
        public void initialize(
            Object value,
            long memoryUsage,
            boolean isReusable);
    }

    /**
     * Interface for a cache entry; same as Map.Entry except that there is no
     * requirement on equals/hashCode. This interface is implemented by
     * FarragoObjectCache; callers are shielded from direct access to
     * the entry representation.
     *
     * <p>Entry extends FarragoAllocation; its closeAllocation implementation
     * calls {@link #unpin}.
     */
    public interface Entry
        extends FarragoAllocation
    {
        /**
         * @return the key of this entry (as passed to the {@link #pin} method).
         */
        public Object getKey();

        /**
         * @return the value cached by this entry
         * (as set by {@link UninitializedEntry#initialize}).
         */
        public Object getValue();
    }
}

// End FarragoObjectCache.java

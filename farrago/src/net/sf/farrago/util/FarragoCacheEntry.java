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

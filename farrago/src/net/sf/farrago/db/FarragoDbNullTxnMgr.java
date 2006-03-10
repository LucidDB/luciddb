/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2006 The Eigenbase Project
// Copyright (C) 2006-2006 Disruptive Tech
// Copyright (C) 2006-2006 LucidEra, Inc.
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
package net.sf.farrago.db;

import net.sf.farrago.session.*;

import java.util.*;
import java.util.concurrent.atomic.*;

import org.eigenbase.relopt.*;

/**
 * FarragoDbNullTxnMgr is a do-nothing implementation of {@link
 * FarragoSessionTxnMgr}.  It is useful as a base class because
 * it has a default implementation for generating new
 * transaction ID's and notifying listeners.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoDbNullTxnMgr implements FarragoSessionTxnMgr
{
    private final AtomicLong nextId;

    private final List<FarragoSessionTxnListener> listeners;
    
    public FarragoDbNullTxnMgr()
    {
        nextId = new AtomicLong(1);
        listeners = new ArrayList<FarragoSessionTxnListener>();
    }

    // implement FarragoSessionTxnMgr
    public void addListener(
        FarragoSessionTxnListener listener)
    {
        listeners.add(listener);
    }
    
    // implement FarragoSessionTxnMgr
    public void removeListener(
        FarragoSessionTxnListener listener)
    {
        listeners.remove(listener);
    }
    
    // implement FarragoSessionTxnMgr
    public FarragoSessionTxnId beginTxn(FarragoSession session)
    {
        FarragoSessionTxnId newId = new LongTxnId(nextId.getAndIncrement());
        for (FarragoSessionTxnListener listener : listeners) {
            listener.transactionBegun(session, newId);
        }
        return newId;
    }

    // implement FarragoSessionTxnMgr
    public void accessTable(
        FarragoSessionTxnId txnId,
        List<String> localTableName,
        TableAccessMap.Mode accessType)
    {
        for (FarragoSessionTxnListener listener : listeners) {
            listener.tableAccessed(txnId, localTableName, accessType);
        }
    }

    // implement FarragoSessionTxnMgr
    public void endTxn(
        FarragoSessionTxnId txnId,
        FarragoSessionTxnEnd endType)
    {
        for (FarragoSessionTxnListener listener : listeners) {
            listener.transactionEnded(txnId, endType);
        }
    }

    private static class LongTxnId implements FarragoSessionTxnId
    {
        private final long id;

        LongTxnId(long id)
        {
            this.id = id;
        }

        public String toString()
        {
            return Long.toString(id);
        }

        public boolean equals(Object obj)
        {
            if (!(obj instanceof LongTxnId)) {
                return false;
            }
            LongTxnId other = (LongTxnId) obj;
            return id == other.id;
        }

        public int hashCode()
        {
            return (int) id;
        }
    }
}

// End FarragoDbNullTxnMgr.java

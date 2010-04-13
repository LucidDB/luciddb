/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006 The Eigenbase Project
// Copyright (C) 2006 SQLstream, Inc.
// Copyright (C) 2006 Dynamo BI Corporation
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

import java.util.*;
import java.util.concurrent.atomic.*;

import net.sf.farrago.session.*;
import net.sf.farrago.type.runtime.*;

import org.eigenbase.relopt.*;


/**
 * FarragoDbNullTxnMgr is a do-nothing implementation of {@link
 * FarragoSessionTxnMgr}. It is useful as a base class because it has a default
 * implementation for generating new transaction ID's and notifying listeners.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoDbNullTxnMgr
    implements FarragoSessionTxnMgr
{
    //~ Instance fields --------------------------------------------------------

    private final AtomicLong nextId;

    private final List<FarragoSessionTxnListener> listeners;

    //~ Constructors -----------------------------------------------------------

    public FarragoDbNullTxnMgr()
    {
        nextId = new AtomicLong(1);
        listeners = new ArrayList<FarragoSessionTxnListener>();
    }

    //~ Methods ----------------------------------------------------------------

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
    public void accessTables(
        FarragoSessionTxnId txnId,
        TableAccessMap accessMap)
    {
        // NOTE jvs 17-Mar-2006: We reorder table accesses to minimize spurious
        // deadlocks.  Take write locks before read locks because read->write
        // upgrade is a very common deadlock.  And sort by table name so that
        // all statements use the same ordering.

        List<List<String>> tableNames =
            new ArrayList<List<String>>(
                accessMap.getTablesAccessed());

        Collections.sort(
            tableNames,
            new CharStringComparator());

        // TODO jvs 17-Mar-2006:  use a LockOrderComparator to do
        // the job more cleanly.

        // First deal with WRITE_ACCESS and READWRITE_ACCESS.
        for (List<String> tableName : tableNames) {
            TableAccessMap.Mode accessType =
                accessMap.getTableAccessMode(tableName);
            if (accessType == TableAccessMap.Mode.READ_ACCESS) {
                continue;
            }
            accessTablePrivate(
                txnId,
                tableName,
                accessType);
        }

        // Then deal with READ_ACCESS.
        for (List<String> tableName : tableNames) {
            TableAccessMap.Mode accessType =
                accessMap.getTableAccessMode(tableName);
            if (accessType != TableAccessMap.Mode.READ_ACCESS) {
                continue;
            }
            accessTablePrivate(
                txnId,
                tableName,
                accessType);
        }
    }

    private void accessTablePrivate(
        FarragoSessionTxnId txnId,
        List<String> localTableName,
        TableAccessMap.Mode accessType)
    {
        for (FarragoSessionTxnListener listener : listeners) {
            listener.tableAccessed(txnId, localTableName, accessType);
        }
        accessTable(txnId, localTableName, accessType);
    }

    /**
     * Called by accessTables for each table accessed. Default implementation is
     * to do nothing; subclasses override this to take real actions such as
     * calling a lock manager.
     *
     * @param txnId ID of accessing transaction
     * @param localTableName qualified name of table as it is known in the local
     * catalog
     * @param accessType type of table access
     */
    protected void accessTable(
        FarragoSessionTxnId txnId,
        List<String> localTableName,
        TableAccessMap.Mode accessType)
    {
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

    //~ Inner Classes ----------------------------------------------------------

    private static class LongTxnId
        implements FarragoSessionTxnId
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

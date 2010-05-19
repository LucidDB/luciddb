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
package net.sf.farrago.catalog;

import java.io.*;

import java.util.*;

import org.netbeans.mdr.persistence.*;
import org.netbeans.mdr.persistence.memoryimpl.*;
import org.netbeans.mdr.util.*;


/**
 * FarragoTransientStorage provides storage for transient MDR objects. Adapted
 * from org.netbeans.mdr.persistence.memoryimpl.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FarragoTransientStorage
    extends StorageImpl
{
    //~ Static fields/initializers ---------------------------------------------

    // NOTE jvs 6-May-2004: Another hack.  This is what actually implements the
    // desired transient effect: when true, commits are converted into
    // rollbacks.  This isn't set until after all storage initialization is
    // completed, so system-defined data stays around permanently.  Need to make
    // this work properly for the system/user catalog split.
    static boolean ignoreCommit;

    //~ Instance fields --------------------------------------------------------

    // NOTE jvs 6-May-2004:  I had to extend StorageImpl to avoid having to
    // copy the entire index classes.  Watch out for the fact that
    // the data members here shadow the unused ones in the superclass!
    private final HashMap maps = new HashMap();
    private PVIndex primaryIndex;
    private Set newIndexes = new HashSet();
    private HashMap removedIndexes = new HashMap();

    //~ Constructors -----------------------------------------------------------

    FarragoTransientStorage()
    {
        super(FarragoTransientStorageFactory.NULL_STORAGE_ID, null);
        ignoreCommit = false;
    }

    //~ Methods ----------------------------------------------------------------

    // implement Storage
    public synchronized void create(
        boolean replace,
        ObjectResolver resolver)
        throws StorageException
    {
        createPrimaryIndex();
    }

    // implement Storage
    public synchronized void close()
        throws StorageException
    {
        shutDown();
    }

    // implement Storage
    public synchronized boolean delete()
        throws StorageException
    {
        return false;
    }

    // implement Storage
    public synchronized boolean exists()
        throws StorageException
    {
        return false;
    }

    // implement Storage
    public synchronized void open(
        boolean createOnNoExist,
        ObjectResolver resolver)
        throws StorageException
    {
        createPrimaryIndex();
    }

    // implement Storage
    public synchronized void objectStateWillChange(Object key)
        throws StorageException
    {
        primaryIndex.willChange(key);
    }

    // implement Storage
    public synchronized void objectStateChanged(Object key)
        throws StorageException
    {
        primaryIndex.changed(key);
    }

    // implement Storage
    public synchronized void rollBackChanges()
        throws StorageException
    {
        // drop all indexes created during the transaction
        Iterator iter = newIndexes.iterator();
        while (iter.hasNext()) {
            maps.remove(iter.next());
        }

        // restore all indexes existing before the transaction that have been
        // removed by the transaction
        iter = removedIndexes.keySet().iterator();
        while (iter.hasNext()) {
            String name = (String) iter.next();
            maps.put(
                name,
                removedIndexes.get(name));
        }

        // call rollback on all indexes
        iter = maps.entrySet().iterator();
        while (iter.hasNext()) {
            TxnIndex index = (TxnIndex) ((Map.Entry) iter.next()).getValue();
            index.rollBackChangesPublic();
        }

        if (primaryIndex != null) {
            primaryIndex.rollBackChangesPublic();
        }
    }

    // implement Storage
    public synchronized void shutDown()
        throws StorageException
    {
        commitChanges();
    }

    // implement Storage
    public synchronized void commitChanges()
        throws StorageException
    {
        if (ignoreCommit) {
            rollBackChanges();
            return;
        }

        newIndexes.clear();
        removedIndexes.clear();

        // call commit on all indexes
        Iterator iter = maps.entrySet().iterator();
        while (iter.hasNext()) {
            TxnIndex index = (TxnIndex) ((Map.Entry) iter.next()).getValue();
            index.commitChangesPublic();
        }
        if (primaryIndex != null) {
            primaryIndex.commitChangesPublic();
        }
    }

    // implement Storage
    public synchronized SinglevaluedIndex getSinglevaluedIndex(String name)
        throws StorageException
    {
        return (SinglevaluedIndex) getIndex(name);
    }

    // implement Storage
    public synchronized MultivaluedIndex getMultivaluedIndex(String name)
        throws StorageException
    {
        return (MultivaluedIndex) getIndex(name);
    }

    // implement Storage
    public synchronized MultivaluedOrderedIndex getMultivaluedOrderedIndex(
        String name)
        throws StorageException
    {
        return (MultivaluedOrderedIndex) getIndex(name);
    }

    // implement Storage
    public synchronized void dropIndex(String name)
        throws StorageException
    {
        Object index = maps.remove(name);
        if ((index != null) && !newIndexes.remove(name)) {
            removedIndexes.put(name, index);
        }
    }

    private synchronized void addIndex(
        String name,
        Index index)
        throws StorageException
    {
        maps.put(name, index);
        newIndexes.add(name);
    }

    // implement Storage
    public synchronized SinglevaluedIndex createSinglevaluedIndex(
        String name,
        EntryType keyType,
        EntryType valueType)
        throws StorageException
    {
        assert (!valueType.equals(EntryType.STREAMABLE));
        SinglevaluedIndex sm = new SVIndex(name, this, keyType, valueType);
        addIndex(name, sm);
        return sm;
    }

    // implement Storage
    public synchronized MultivaluedOrderedIndex createMultivaluedOrderedIndex(
        String name,
        EntryType keyType,
        EntryType valueType,
        boolean unique)
        throws StorageException
    {
        MultivaluedOrderedIndex sm =
            new MVIndex(name, this, keyType, valueType, unique);
        addIndex(name, sm);
        return sm;
    }

    // implement Storage
    public synchronized MultivaluedIndex createMultivaluedIndex(
        String name,
        EntryType keyType,
        EntryType valueType,
        boolean unique)
        throws StorageException
    {
        MultivaluedIndex sm =
            new MVIndex(name, this, keyType, valueType, unique);
        addIndex(name, sm);
        return sm;
    }

    // implement Storage
    public synchronized SinglevaluedIndex getPrimaryIndex()
        throws StorageException
    {
        return primaryIndex;
    }

    // implement Storage
    private void createPrimaryIndex()
        throws StorageException
    {
        primaryIndex = new PVIndex(this);
    }

    // implement Storage
    public synchronized Index getIndex(String name)
        throws StorageException
    {
        return (Index) maps.get(name);
    }

    //~ Inner Interfaces -------------------------------------------------------

    private static interface TxnIndex
    {
        public void commitChangesPublic()
            throws StorageException;

        public void rollBackChangesPublic()
            throws StorageException;
    }

    //~ Inner Classes ----------------------------------------------------------

    private static class PVIndex
        extends PrimaryIndexImpl
        implements TxnIndex
    {
        PVIndex(StorageImpl storage)
        {
            super(storage);
        }

        public void commitChangesPublic()
            throws StorageException
        {
            commitChanges();
        }

        public void rollBackChangesPublic()
            throws StorageException
        {
            rollBackChanges();
        }
    }

    private static class SVIndex
        extends SinglevaluedIndexImpl
        implements TxnIndex
    {
        SVIndex(
            String name,
            StorageImpl storage,
            Storage.EntryType keyType,
            Storage.EntryType valueType)
        {
            super(name, storage, keyType, valueType);
        }

        public void commitChangesPublic()
            throws StorageException
        {
            commitChanges();
        }

        public void rollBackChangesPublic()
            throws StorageException
        {
            rollBackChanges();
        }
    }

    private static class MVIndex
        extends MultivaluedOrderedIndexImpl
        implements TxnIndex
    {
        public MVIndex(
            String name,
            StorageImpl storage,
            Storage.EntryType keyType,
            Storage.EntryType valueType,
            boolean unique)
        {
            super(name, storage, keyType, valueType, unique);
        }

        public void commitChangesPublic()
            throws StorageException
        {
            commitChanges();
        }

        public void rollBackChangesPublic()
            throws StorageException
        {
            rollBackChanges();
        }
    }
}

// End FarragoTransientStorage.java

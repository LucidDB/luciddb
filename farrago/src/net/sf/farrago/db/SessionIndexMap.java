/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/
package net.sf.farrago.db;

import java.sql.*;
import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fennel.*;
import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.util.*;
import net.sf.farrago.query.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.type.*;
import net.sf.farrago.util.*;


/**
 * SessionIndexMap implements FarragoIndexMap, resolving indexes for both
 * permanent and temporary tables.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class SessionIndexMap extends FarragoCompoundAllocation
    implements FarragoIndexMap
{
    //~ Instance fields -------------------------------------------------------

    private FarragoDatabase database;

    /**
     * Map from index to root PageId for temporary tables.
     */
    private Map tempIndexRootMap;

    /**
     * Map from index ID to index for all tables.
     */
    private Map indexIdMap;

    /**
     * Repos for this session.
     */
    private FarragoRepos repos;

    /**
     * Cache for local data wrappers used to manage indexes.
     */
    private FarragoDataWrapperCache dataWrapperCache;

    //~ Constructors ----------------------------------------------------------

    /**
     * Create a new SessionIndexMap.
     *
     * @param owner FarragoAllocationOwner which will own this map; on
     * closeAllocation, any temporary indexes will be deleted automatically
     *
     * @param database FarragoDatabase context
     *
     * @param repos the repos for this session
     */
    public SessionIndexMap(
        FarragoAllocationOwner owner,
        FarragoDatabase database,
        FarragoRepos repos)
    {
        this.database = database;
        this.repos = repos;
        tempIndexRootMap = new HashMap();
        indexIdMap = new HashMap();
        owner.addAllocation(this);

        dataWrapperCache =
            new FarragoDataWrapperCache(
                this,
                database.getDataWrapperCache(),
                repos,
                database.getFennelDbHandle());
    }

    //~ Methods ---------------------------------------------------------------

    // implement FarragoIndexMap
    public long getIndexRoot(CwmSqlindex index)
    {
        if (repos.isTemporary(index)) {
            Long root = (Long) tempIndexRootMap.get(index);
            assert (root != null);
            return root.longValue();
        } else {
            return Long.parseLong(repos.getTagValue(index, "indexRoot"));
        }
    }

    private void setIndexRoot(
        CwmSqlindex index,
        long root)
    {
        if (repos.isTemporary(index)) {
            Object old = tempIndexRootMap.put(
                    index,
                    new Long(root));
            assert (old == null);
        } else {
            repos.setTagValue(
                index,
                "indexRoot",
                Long.toString(root));
        }
    }

    // implement FarragoIndexMap
    public void instantiateTemporaryTable(CwmTable table)
    {
        assert (table.isTemporary());

        CwmSqlindex clusteredIndex = repos.getClusteredIndex(table);

        if (tempIndexRootMap.containsKey(clusteredIndex)) {
            // already instantiated this table
            return;
        }

        Iterator iter = repos.getIndexes(table).iterator();
        while (iter.hasNext()) {
            CwmSqlindex index = (CwmSqlindex) iter.next();
            assert (!tempIndexRootMap.containsKey(index));
            createIndexStorage(index);
        }
    }

    // implement FarragoAllocation
    public void closeAllocation()
    {
        // materialize deletion list to avoid ConcurrentModificationException
        List list = new ArrayList(tempIndexRootMap.keySet());
        Iterator iter = list.iterator();
        while (iter.hasNext()) {
            CwmSqlindex index = (CwmSqlindex) iter.next();
            dropIndexStorage(index, false);
        }

        // TODO:  make Fennel drop temporary indexes on recovery also
        // NOTE:  do this last, so that we don't release data wrappers
        // until we're done using them for drops above
        super.closeAllocation();
    }

    /**
     * Commit hook:  truncate any indexes for tables defined with
     * ON COMMIT DELETE ROWS.
     */
    public void onCommit()
    {
        Iterator iter = tempIndexRootMap.keySet().iterator();
        while (iter.hasNext()) {
            CwmSqlindexImpl index = (CwmSqlindexImpl) iter.next();
            if (index.getTable().getTemporaryScope().endsWith("PRESERVE")) {
                continue;
            }
            dropIndexStorage(index, true);
        }
    }

    // REVIEW:  rollback issues
    // implement FarragoIndexMap
    public void createIndexStorage(CwmSqlindex index)
    {
        FarragoMedLocalDataServer server = getIndexDataServer(index);
        long indexRoot;
        try {
            indexRoot = server.createIndex(index);
        } catch (SQLException ex) {
            throw FarragoResource.instance().newDataServerIndexCreateFailed(
                repos.getLocalizedObjectName(index, null),
                ex);
        }
        setIndexRoot(index, indexRoot);
        indexIdMap.put(
            new Long(JmiUtil.getObjectId(index)),
            index);
    }

    // implement FarragoIndexMap
    public void dropIndexStorage(
        CwmSqlindex index,
        boolean truncate)
    {
        if (repos.isTemporary(index)) {
            if (!tempIndexRootMap.containsKey(index)) {
                // index was never created, so nothing to do
                return;
            }
        }

        FarragoMedLocalDataServer server = getIndexDataServer(index);
        try {
            server.dropIndex(
                index,
                getIndexRoot(index),
                truncate);
        } catch (SQLException ex) {
            throw FarragoResource.instance().newDataServerIndexDropFailed(
                repos.getLocalizedObjectName(index, null),
                ex);
        }

        if (!truncate) {
            indexIdMap.remove(new Long(JmiUtil.getObjectId(index)));
            tempIndexRootMap.remove(index);
        }
    }

    // implement FarragoIndexMap
    public CwmSqlindex getIndexById(long id)
    {
        return (CwmSqlindex) indexIdMap.get(new Long(id));
    }

    private FarragoMedLocalDataServer getIndexDataServer(CwmSqlindex index)
    {
        FemLocalTable localTable = (FemLocalTable) index.getSpannedClass();
        FemDataServerImpl femServer =
            (FemDataServerImpl) localTable.getServer();
        return (FarragoMedLocalDataServer) femServer.loadFromCache(dataWrapperCache);
    }
}


// End SessionIndexMap.java

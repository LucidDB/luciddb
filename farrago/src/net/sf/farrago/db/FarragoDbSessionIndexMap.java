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
package net.sf.farrago.db;

import java.sql.SQLException;
import java.util.*;

import org.eigenbase.jmi.JmiObjUtil;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.relational.CwmTable;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.util.FarragoDataWrapperCache;
import net.sf.farrago.resource.FarragoResource;
import net.sf.farrago.session.FarragoSessionIndexMap;
import net.sf.farrago.util.*;


/**
 * FarragoDbSessionIndexMap implements {@link FarragoSessionIndexMap}, resolving
 * indexes for both permanent and temporary tables.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FarragoDbSessionIndexMap
    extends FarragoCompoundAllocation
    implements FarragoSessionIndexMap
{
    //~ Instance fields --------------------------------------------------------

    private FarragoDbSession dbSession;

    /**
     * Map from index MOF ID to root PageId for temporary tables.
     */
    private Map<String, Long> tempIndexRootMap;

    /**
     * Repos for this session.
     */
    private FarragoRepos repos;

    /**
     * Cache for local data wrappers used to manage indexes.
     */
    private FarragoDataWrapperCache privateDataWrapperCache;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new FarragoDbSessionIndexMap.
     *
     * @param owner FarragoAllocationOwner which will own this map; on
     * closeAllocation, any temporary indexes will be deleted automatically
     * @param dbSession FarragoDbSession context
     * @param repos the repos for this session
     */
    public FarragoDbSessionIndexMap(
        FarragoAllocationOwner owner,
        FarragoDbSession dbSession,
        FarragoRepos repos)
    {
        this.dbSession = dbSession;
        this.repos = repos;
        tempIndexRootMap = new HashMap<String, Long>();
        owner.addAllocation(this);

        privateDataWrapperCache =
            new FarragoDataWrapperCache(
                this,
                dbSession.getDatabase().getDataWrapperCache(),
                dbSession.getDatabase().getPluginClassLoader(),
                repos,
                dbSession.getDatabase().getFennelDbHandle(),
                null);
    }

    //~ Methods ----------------------------------------------------------------

    // implement FarragoSessionIndexMap
    public long getIndexRoot(FemLocalIndex index)
    {
        return getIndexRoot(index, false);
    }

    // implement FarragoSessionIndexMap
    public long getIndexRoot(FemLocalIndex index, boolean write)
    {
        if (FarragoCatalogUtil.isIndexTemporary(index)) {
            Long root = tempIndexRootMap.get(index.refMofId());
            assert (root != null);
            return root.longValue();
        } else {
            return Long.parseLong(index.getStorageId());
        }
    }

    // implement FarragoSessionIndexMap
    public void setIndexRoot(
        FemLocalIndex index,
        long root)
    {
        if (FarragoCatalogUtil.isIndexTemporary(index)) {
            Long old =
                tempIndexRootMap.put(
                    index.refMofId(),
                    new Long(root));
            assert (old == null);
        } else {
            index.setStorageId(Long.toString(root));
        }
    }

    // implement FarragoSessionIndexMap
    public void instantiateTemporaryTable(
        FarragoDataWrapperCache wrapperCache,
        CwmTable table)
    {
        assert (table.isTemporary());

        FemLocalIndex clusteredIndex =
            FarragoCatalogUtil.getClusteredIndex(repos, table);

        if (tempIndexRootMap.containsKey(clusteredIndex.refMofId())) {
            // already instantiated this table
            return;
        }

        for (
            FemLocalIndex index
            : FarragoCatalogUtil.getTableIndexes(repos, table))
        {
            assert (!tempIndexRootMap.containsKey(index.refMofId()));
            createIndexStorage(wrapperCache, index);
        }
    }

    // implement FarragoAllocation
    public void closeAllocation()
    {
        FarragoReposTxnContext txn = repos.newTxnContext(true);
        txn.beginReadTxn();
        try {
            // materialize deletion list to avoid ConcurrentModificationException
            List<String> list =
                new ArrayList<String>(tempIndexRootMap.keySet());
            for (String indexMofId : list) {
                FemLocalIndex index = 
                    (FemLocalIndex) repos.getMdrRepos().getByMofId(indexMofId);
                dropIndexStorage(privateDataWrapperCache, index, false);
            }
        }
        finally {
            txn.commit();
        }
        
        // TODO:  make Fennel drop temporary indexes on recovery also
        // NOTE:  do this last, so that we don't release data wrappers
        // until we're done using them for drops above
        super.closeAllocation();
    }

    /**
     * Commit hook: truncates any indexes for tables defined with ON COMMIT
     * DELETE ROWS.
     */
    public void onCommit()
    {
        for (String indexMofId : tempIndexRootMap.keySet()) {
            FemLocalIndex index = 
                (FemLocalIndex) repos.getMdrRepos().getByMofId(indexMofId);

            String temporaryScope =
                FarragoCatalogUtil.getIndexTable(index).getTemporaryScope();
            if (temporaryScope.endsWith("PRESERVE")) {
                continue;
            }
            dropIndexStorage(privateDataWrapperCache, index, true);
        }
    }

    // implement FarragoSessionIndexMap
    public void createIndexStorage(
        FarragoDataWrapperCache wrapperCache,
        FemLocalIndex index)
    {
        createIndexStorage(wrapperCache, index, true);
    }

    // REVIEW:  rollback issues
    // implement FarragoSessionIndexMap
    public long createIndexStorage(
        FarragoDataWrapperCache wrapperCache,
        FemLocalIndex index,
        boolean updateMap)
    {
        FarragoMedLocalDataServer server =
            getIndexDataServer(wrapperCache, index);
        long indexRoot;
        try {
            indexRoot =
                server.createIndex(index, dbSession.getFennelTxnContext());
        } catch (SQLException ex) {
            throw FarragoResource.instance().DataServerIndexCreateFailed.ex(
                repos.getLocalizedObjectName(index),
                ex);
        }
        if (updateMap) {
            setIndexRoot(index, indexRoot);
        }
        
        return indexRoot;
    }

    // implement FarragoSessionIndexMap
    public void dropIndexStorage(
        FarragoDataWrapperCache wrapperCache,
        FemLocalIndex index,
        boolean truncate)
    {
        if (FarragoCatalogUtil.isIndexTemporary(index)) {
            if (!tempIndexRootMap.containsKey(index.refMofId())) {
                // index was never created, so nothing to do
                return;
            }
        }

        FarragoMedLocalDataServer server =
            getIndexDataServer(wrapperCache, index);
        try {
            server.dropIndex(
                index,
                getIndexRoot(index),
                truncate,
                dbSession.getFennelTxnContext());
        } catch (SQLException ex) {
            throw FarragoResource.instance().DataServerIndexDropFailed.ex(
                repos.getLocalizedObjectName(index),
                ex);
        }

        if (!truncate) {
            tempIndexRootMap.remove(index.refMofId());
        }
    }

    // implement FarragoSessionIndexMap
    public FarragoMedLocalIndexStats computeIndexStats(
        FarragoDataWrapperCache wrapperCache,
        FemLocalIndex index,
        boolean estimate)
    {
        FarragoMedLocalDataServer server =
            getIndexDataServer(wrapperCache, index);
        try {
            return server.computeIndexStats(
                index,
                getIndexRoot(index),
                estimate,
                dbSession.getFennelTxnContext());
        } catch (SQLException ex) {
            throw FarragoResource.instance().DataServerIndexVerifyFailed.ex(
                repos.getLocalizedObjectName(index),
                ex);
        }
    }

    // implement FarragoSessionIndexMap
    public FemLocalIndex getIndexById(long id)
    {
        String mofId = JmiObjUtil.toMofId(id);

        FemLocalIndex index = 
            (FemLocalIndex) repos.getMdrRepos().getByMofId(mofId);
        
        return index;
    }

    private FarragoMedLocalDataServer getIndexDataServer(
        FarragoDataWrapperCache wrapperCache,
        FemLocalIndex index)
    {
        FemLocalTable localTable = (FemLocalTable) index.getSpannedClass();
        return (FarragoMedLocalDataServer) wrapperCache.loadServerFromCatalog(
            localTable.getServer());
    }

    public void versionIndexRoot(
        FarragoDataWrapperCache wrapperCache,
        FemLocalIndex index,
        Long newRoot)
    {
        FarragoMedLocalDataServer server =
            getIndexDataServer(wrapperCache, index);
        try {
            // Truncate the original index and then version the new index
            // root to the now empty root.  Note that we cannot drop the
            // original index because that would result in losing the
            // original root page that we need to version.
            dropIndexStorage(wrapperCache, index, true);
            server.versionIndexRoot(
                getIndexRoot(index),
                newRoot,
                dbSession.getFennelTxnContext());
        } catch (SQLException ex) {
            throw FarragoResource.instance().DataServerIndexVersionFailed.ex(
                repos.getLocalizedObjectName(index),
                ex);
        }
    }
}

// End FarragoDbSessionIndexMap.java

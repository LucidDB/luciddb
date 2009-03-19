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
package net.sf.farrago.db;

import java.sql.*;

import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.util.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.session.*;
import net.sf.farrago.util.*;

import org.eigenbase.jmi.*;


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

    // implement FarragoSessionIndexMap
    public CwmTable getReloadTable()
    {
        return null;
    }

    // implement FarragoSessionIndexMap
    public CwmTable getOldTableStructure()
    {
        return null;
    }

    // implement FarragoAllocation
    public void closeAllocation()
    {
        repos.beginReposSession();
        try {
            // materialize deletion list to avoid
            // ConcurrentModificationException
            List<String> list =
                new ArrayList<String>(tempIndexRootMap.keySet());
            for (String indexMofId : list) {
                dropIndexStorage(privateDataWrapperCache, indexMofId, false);
            }

            // TODO:  make Fennel drop temporary indexes on recovery also
            // NOTE:  do this last, so that we don't release data wrappers
            // until we're done using them for drops above
            super.closeAllocation();
        } finally {
            repos.endReposSession();
        }
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
            dropIndexStorage(privateDataWrapperCache, indexMofId, true);
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
        dropIndexStorageImpl(wrapperCache, index, null, truncate);
    }

    // implement FarragoSessionIndexMap
    public void dropIndexStorage(
        FarragoDataWrapperCache wrapperCache,
        String indexMofId,
        boolean truncate)
    {
        dropIndexStorageImpl(wrapperCache, null, indexMofId, truncate);
    }

    private void dropIndexStorageImpl(
        FarragoDataWrapperCache wrapperCache,
        FemLocalIndex index,
        String indexMofId,
        boolean truncate)
    {
        FarragoReposTxnContext txn = repos.newTxnContext(true);
        if (index == null) {
            assert (indexMofId != null);
            txn.beginReadTxn();
            index = (FemLocalIndex) repos.getMdrRepos().getByMofId(indexMofId);
        } else {
            assert (indexMofId == null);
            indexMofId = index.refMofId();
        }
        try {
            if (FarragoCatalogUtil.isIndexTemporary(index)) {
                if (!tempIndexRootMap.containsKey(indexMofId)) {
                    // index was never created, so nothing to do
                    return;
                }
            }

            FarragoMedLocalDataServer server =
                getIndexDataServer(wrapperCache, index);
            String localizedIndexName = repos.getLocalizedObjectName(index);
            try {
                long root = getIndexRoot(index);

                // The dropIndex call is the potentially long-running part, so
                // before invoking it, end our repository transaction if we
                // started one.  We've already been careful to dig out anything
                // we need from the repository by this point.
                txn.commit();

                // FIXME jvs 11-Dec-2008:  We're still passing in
                // the index reference here.  The local data wrapper
                // SPI needs to be fixed to avoid this.  We're probably
                // OK for now since the calls so far
                // (e.g. FarragoCatalogUtil.isIndexTemporary) will
                // have preloaded references needed.
                server.dropIndex(
                    index,
                    root,
                    truncate,
                    dbSession.getFennelTxnContext());
            } catch (SQLException ex) {
                throw FarragoResource.instance().DataServerIndexDropFailed.ex(
                    localizedIndexName,
                    ex);
            }

            if (!truncate) {
                tempIndexRootMap.remove(indexMofId);
            }
        } finally {
            txn.commit();
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

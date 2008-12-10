/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2008-2008 The Eigenbase Project
// Copyright (C) 2008-2008 Disruptive Tech
// Copyright (C) 2008-2008 LucidEra, Inc.
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
package net.sf.farrago.ddl;

import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.defimpl.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.util.*;
import net.sf.farrago.session.*;
import net.sf.farrago.util.*;

import org.eigenbase.sql.*;
import org.eigenbase.sql.pretty.*;
import org.eigenbase.util.*;

/**
 * DdlReloadTableStmt is an abstract base for statements which
 * need to self-insert data from an existing table.  Currently
 * this includes ALTER TABLE REBUILD and ALTER TABLE ADD COLUMN.
 *
 * @author John Pham
 * @author John Sichi
 * @version $Id$
 */
public abstract class DdlReloadTableStmt
    extends DdlStmt
    implements DdlMultipleTransactionStmt
{
    //~ Instance fields --------------------------------------------------------

    private CwmTable table;

    private FarragoSessionIndexMap baseIndexMap;
    private FarragoDataWrapperCache wrapperCache;
    private Map<FemLocalIndex, Long> writeIndexMap;
    private FarragoSessionIndexMap rebuildMap;
    private String reloadSql;
    private boolean rebuildingIndexes;
    protected FemRecoveryReference recoveryRef;

    //~ Constructors -----------------------------------------------------------

    /**
     * Constructs a DdlReloadTableStmt.
     *
     * @param table target table
     */
    public DdlReloadTableStmt(CwmTable table)
    {
        super(table, true);
        this.table = table;
    }

    //~ Methods ----------------------------------------------------------------

    // implement DdlStmt
    public void visit(DdlVisitor visitor)
    {
        visitor.visit(this);
    }

    // implement DdlMultipleTransactionStmt
    public void prepForExecuteUnlocked(
        FarragoSessionDdlValidator ddlValidator,
        FarragoSession session)
    {
        FarragoRepos repos = session.getRepos();
        wrapperCache = ddlValidator.getDataWrapperCache();
        SqlDialect dialect = new SqlDialect(session.getDatabaseMetaData());
        SqlPrettyWriter writer = new SqlPrettyWriter(dialect);
        
        rebuildingIndexes = shouldRebuildIndexes(ddlValidator);
        writeIndexMap = new HashMap<FemLocalIndex, Long>();
        baseIndexMap = ddlValidator.getIndexMap();
        
        // Create new index roots, which depends on index creation validation
        // events being triggered
        if (rebuildingIndexes) {
            for (
                FemLocalIndex index
                    : FarragoCatalogUtil.getTableIndexes(repos, table))
            {
                // Keep the old deletion index, because it will not be loaded
                if (FarragoCatalogUtil.isDeletionIndex(index)) {
                    continue;
                }
                long newRoot =
                    baseIndexMap.createIndexStorage(wrapperCache, index, false);
                writeIndexMap.put(index, newRoot);
            }
        }
        rebuildMap = new ReloadTableIndexMap(baseIndexMap, writeIndexMap);

        // REVIEW jvs 4-Dec-2008:  for ALTER TABLE ADD COLUMN, it
        // would be nice to avoid touching the rowcounts altogether,
        // since they won't actually change.

        // Reset current row counts, if applicable to the personality.
        //
        // REVIEW zfong 3/27/07 - Note that the update to the rowcounts below,
        // as well as the one that will be done as part of the insert-select
        // that follows, are executed in separate MDR xacts.  This will
        // result in inconsistent rowcounts if a failure occurs in the
        // middle of the alter table rebuild.  The alternative is to hold
        // the MDR lock for the duration of the entire alter table rebuild.
        // Or another would be to find a way to defer updating the rowcounts.
        session.getPersonality().resetRowCounts((FemAbstractColumnSet) table);

        // REVIEW jvs 5-Dec-2009:  For FTRS ALTER TABLE REBUILD, the
        // next step is necessary, since the roots actually change
        // in the catalog, but for LucidDB with page versioning
        // enabled, it seems superfluous since we just version
        // the existing roots (so existing plans remain valid).
        
        // Update the table's timestamp (for a normal DdlStmt executing in
        // preValidate this happens as a result of DdlValidator's event
        // monitoring).  It's necessary so that any cached plans involving this
        // table will expire, but it's hokey since for ALTER TABLE REBUILD the
        // table definition hasn't actually changed.
        FarragoCatalogUtil.updateAnnotatedElement(
            (FemLocalTable)table,
            ddlValidator.obtainTimestamp(),
            false);
        
        reloadSql = getReloadDml(writer);
    }
    
    // implement DdlMultipleTransactionStmt
    public void executeUnlocked(
        FarragoSessionDdlValidator ddlValidator,
        FarragoSession session)
    {
        // We'll copy data from old roots to new roots
        session.setSessionIndexMap(rebuildMap);

        // Instrumentation for concurrency tests
        sleepIfTrapSet();

        session.getSessionVariables().set(
            FarragoDefaultSessionPersonality.CACHE_STATEMENTS,
            Boolean.toString(false));
        session.getSessionVariables().set(
            FarragoDefaultSessionPersonality.ENFORCE_IDENTITY_GENERATED_ALWAYS,
            Boolean.toString(false));
        FarragoSessionStmtContext stmtContext = session.newStmtContext(null);
        boolean success = false;
        stmtContext.prepare(reloadSql, true);
        stmtContext.execute();
    }

    private void sleepIfTrapSet()
    {
        if (!FarragoProperties.instance().testTableReloadSleep.isSet()) {
            return;
        }
        int millis =
            FarragoProperties.instance().testTableReloadSleep.get();
        try {
            Thread.currentThread().sleep(millis);
        } catch (InterruptedException ex) {
            throw Util.newInternal(ex);
        } finally {
            // one shot auto-reset trap
            FarragoProperties.instance().remove(
                FarragoProperties.instance().testTableReloadSleep.getPath());
        }
    }

    // implement DdlMultipleTransactionStmt
    public boolean completeRequiresWriteTxn()
    {
        return true;
    }

    /**
     * Called after an exception is thrown during the execution phase.
     *
     * @param ddlValidator DDL validator for this statement
     * @param session reentrant Farrago session
     */
    protected void recoverFromFailure(
        FarragoSessionDdlValidator ddlValidator,
        FarragoSession session)
    {
        // FIXME jvs 8-Dec-2008:  we should be freeing
        // the new roots so that they don't stay around as
        // garbage; this applies to both FTRS and LCS,
        // and to both REBUILD and ADD COLUMN.  Also,
        // for LCS, we need to restore the rowcounts, as noted
        // above by Zelaine.
    }
    
    // implement DdlMultipleTransactionStmt
    public void completeAfterExecuteUnlocked(
        FarragoSessionDdlValidator ddlValidator,
        FarragoSession session,
        boolean success)
    {
        crashIfTrapSet();

        if (recoveryRef != null) {
            // Regardless of success or failure, delete the recoveryRef
            // if it exists, since it is only for crash recovery.
            recoveryRef.refDelete();
            recoveryRef = null;
        }
        
        if (!success) {
            recoverFromFailure(ddlValidator, session);
            return;
        }
        
        FarragoRepos repos = session.getRepos();
        for (
            FemLocalIndex index
                : FarragoCatalogUtil.getTableIndexes(repos, table))
        {
            if (index.isInvalid()) {
                // Indicate that we've successfully built the index.
                index.setInvalid(false);
            }
            if (!rebuildingIndexes) {
                // The rest of the loop is only applicable when
                // rebuilding indexes.
                continue;
            }
            if (FarragoCatalogUtil.isDeletionIndex(index)) {
                // Truncate the deletion index
                baseIndexMap.dropIndexStorage(wrapperCache, index, true);
            } else {
                // Let each personality decide how to update the index
                session.getPersonality().updateIndexRoot(
                    index,
                    wrapperCache,
                    baseIndexMap,
                    writeIndexMap.get(index));
                
                // Update the index's timestamp (for a normal DdlStmt executing
                // in preValidate this happens behind the scenes).
                FarragoCatalogUtil.updateAnnotatedElement(
                    index,
                    ddlValidator.obtainTimestamp(),
                    false);
            }
        }
    }

    private void crashIfTrapSet()
    {
        if (!FarragoProperties.instance().testTableReloadCrash.get()) {
            return;
        }

        // one shot auto-reset trap
        FarragoProperties.instance().remove(
            FarragoProperties.instance().testTableReloadCrash.getPath());
        
        throw new RuntimeException("simulating ALTER TABLE crash");
    }

    /**
     * Determines whether statement execution should rebuild all
     * indexes on the table.  Some ooptimzed reloads may be
     * able to avoid this.
     *
     * @param ddlValidator validator for this DDL statement
     * 
     * @return whether to rebuild
     */
    protected boolean shouldRebuildIndexes(
        FarragoSessionDdlValidator ddlValidator)
    {
        return true;
    }

    /**
     * Generates the SQL to be used to reload the table.
     *
     * @param writer writer to uses for generating SQL
     *
     * @return DML statement which accomplishes reload
     */
    protected abstract String getReloadDml(SqlPrettyWriter writer);

    /**
     * @return the table affected by this statement
     */
    public CwmTable getTable()
    {
        return table;
    }

    /**
     * @return the old table structure if the table structure
     * is being altered, or null if no change is taking place
     */
    protected CwmTable getOldTableStructureForIndexMap()
    {
        return null;
    }
    
    //~ Inner Classes ----------------------------------------------------------

    /**
     * A special index map used when rebuildIndexes=true. This index map
     * overrides index roots for writes, allowing reload queries such as:
     *
     * <pre>"insert into t select * from t"</pre>
     *
     * to copy data from old roots to new roots.
     */
    private class ReloadTableIndexMap
        implements FarragoSessionIndexMap
    {
        private FarragoSessionIndexMap internalMap;
        private Map<FemLocalIndex, Long> writeIndexMap;

        /**
         * Constructs a ReloadTableIndexMap as a wrapper around a standard
         * index map.
         *
         * @param internalMap the original index map
         * @param writeIndexMap a mapping of roots to be returned for writes
         */
        public ReloadTableIndexMap(
            FarragoSessionIndexMap internalMap,
            Map<FemLocalIndex, Long> writeIndexMap)
        {
            this.internalMap = internalMap;
            this.writeIndexMap = writeIndexMap;
        }

        // implement FarragoSessionIndexMap
        public FemLocalIndex getIndexById(long id)
        {
            return internalMap.getIndexById(id);
        }

        // implement FarragoSessionIndexMap
        public long getIndexRoot(FemLocalIndex index)
        {
            return internalMap.getIndexRoot(index);
        }

        // implement FarragoSessionIndexMap
        public long getIndexRoot(FemLocalIndex index, boolean write)
        {
            if (write) {
                Long root = writeIndexMap.get(index);
                if (root != null) {
                    return root;
                }
            }
            return getIndexRoot(index);
        }

        // implement FarragoSessionIndexMap
        public void setIndexRoot(FemLocalIndex index, long pageId)
        {
            internalMap.setIndexRoot(index, pageId);
        }

        // implement FarragoSessionIndexMap
        public void instantiateTemporaryTable(
            FarragoDataWrapperCache wrapperCache,
            CwmTable table)
        {
            internalMap.instantiateTemporaryTable(wrapperCache, table);
        }
        
        // implement FarragoSessionIndexMap
        public CwmTable getOldTableStructure()
        {
            return getOldTableStructureForIndexMap();
        }

        // implement FarragoSessionIndexMap
        public void createIndexStorage(
            FarragoDataWrapperCache wrapperCache,
            FemLocalIndex index)
        {
            internalMap.createIndexStorage(
                wrapperCache,
                index);
        }

        // implement FarragoSessionIndexMap
        public long createIndexStorage(
            FarragoDataWrapperCache wrapperCache,
            FemLocalIndex index,
            boolean updateMap)
        {
            return internalMap.createIndexStorage(
                wrapperCache,
                index,
                updateMap);
        }

        // implement FarragoSessionIndexMap
        public void dropIndexStorage(
            FarragoDataWrapperCache wrapperCache,
            FemLocalIndex index,
            boolean truncate)
        {
            internalMap.dropIndexStorage(wrapperCache, index, truncate);
        }

        // implement FarragoSessionIndexMap
        public FarragoMedLocalIndexStats computeIndexStats(
            FarragoDataWrapperCache wrapperCache,
            FemLocalIndex index,
            boolean estimate)
        {
            return internalMap.computeIndexStats(wrapperCache, index, estimate);
        }

        // implement FarragoSessionIndexMap
        public void onCommit()
        {
            internalMap.onCommit();
        }

        // implement FarragoSesssionIndexMap
        public void versionIndexRoot(
            FarragoDataWrapperCache wrapperCache,
            FemLocalIndex index,
            Long newRoot)
        {
            internalMap.versionIndexRoot(wrapperCache, index, newRoot);
        }
    }
}

// End DdlReloadTableStmt.java

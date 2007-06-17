/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
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
import net.sf.farrago.namespace.util.*;
import net.sf.farrago.session.*;

import org.eigenbase.sql.*;
import org.eigenbase.sql.pretty.*;


/**
 * DdlRebuildTableStmt represents an ALTER TABLE ... REBUILD statement. The
 * statement compacts data stored in a table's indexes by removing deleted
 * entries.
 *
 * @author John Pham
 * @version $Id$
 */
public class DdlRebuildTableStmt
    extends DdlAlterStmt
{
    //~ Instance fields --------------------------------------------------------

    private CwmTable table;

    //~ Constructors -----------------------------------------------------------

    /**
     * Constructs a DdlRebuildTableStmt
     */
    public DdlRebuildTableStmt(CwmTable table)
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

    // implement DdlAlterStmt
    public void execute(
        FarragoSessionDdlValidator ddlValidator,
        FarragoSession session)
    {
        FarragoRepos repos = session.getRepos();
        FarragoSessionIndexMap baseIndexMap = ddlValidator.getIndexMap();
        FarragoDataWrapperCache wrapperCache =
            ddlValidator.getDataWrapperCache();
        SqlDialect dialect = new SqlDialect(session.getDatabaseMetaData());
        SqlPrettyWriter writer = new SqlPrettyWriter(dialect);

        // Create new index roots
        Map<FemLocalIndex, Long> writeIndexMap =
            new HashMap<FemLocalIndex, Long>();
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

        // Copy data from old roots to new roots
        FarragoSessionIndexMap rebuildMap =
            new RebuildTableIndexMap(baseIndexMap, writeIndexMap);
        session.setSessionIndexMap(rebuildMap);
        session.getSessionVariables().set(
            FarragoDefaultSessionPersonality.CACHE_STATEMENTS,
            Boolean.toString(false));
        session.getSessionVariables().set(
            FarragoDefaultSessionPersonality.ENFORCE_IDENTITY_GENERATED_ALWAYS,
            Boolean.toString(false));
        FarragoSessionStmtContext stmtContext = session.newStmtContext(null);
        stmtContext.prepare(getRebuildDml(writer), true);
        stmtContext.execute();

        FarragoReposTxnContext txn = repos.newTxnContext();
        try {
            txn.beginWriteTxn();

            for (
                FemLocalIndex index
                : FarragoCatalogUtil.getTableIndexes(repos, table))
            {
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
                }
            }
            txn.commit();
        } finally {
            txn.rollback();
        }
    }

    /**
     * Generates the query: "insert into T select * from T"
     */
    private String getRebuildDml(SqlPrettyWriter writer)
    {
        SqlIdentifier tableName = FarragoCatalogUtil.getQualifiedName(table);

        writer.print("insert into ");
        tableName.unparse(writer, 0, 0);
        writer.print(" select * from ");
        tableName.unparse(writer, 0, 0);
        String sql = writer.toString();
        return sql;
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * A special index map used by the rebuild table command. This index map
     * overrides index roots for writes, allowing the rebuild query:
     *
     * <pre>"insert into t select * from t"</pre>
     *
     * to copy data from old roots to new roots.
     */
    private class RebuildTableIndexMap
        implements FarragoSessionIndexMap
    {
        private FarragoSessionIndexMap internalMap;
        private Map<FemLocalIndex, Long> writeIndexMap;

        /**
         * Constructs a RebuildTableIndexMap as a wrapper around a standard
         * index map.
         *
         * @param internalMap the original index map
         * @param writeIndexMap a mapping of roots to be returned for writes
         */
        public RebuildTableIndexMap(
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
        public long computeIndexStats(
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

// End DdlAnalyzeStmt.java

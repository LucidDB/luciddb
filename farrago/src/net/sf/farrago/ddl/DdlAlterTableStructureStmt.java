/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2008-2009 The Eigenbase Project
// Copyright (C) 2008-2009 SQLstream, Inc.
// Copyright (C) 2008-2009 LucidEra, Inc.
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

import javax.jmi.reflect.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.core.*;
import net.sf.farrago.cwm.datatypes.*;
import net.sf.farrago.cwm.keysindexes.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fem.security.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.util.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.session.*;

import org.eigenbase.jmi.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.pretty.*;


/**
 * DdlAlterTableStructureStmt represents an ALTER TABLE statement which
 * adds/drops columns or changes their datatype, implying that stored tuples may
 * need to be reshaped. Currently it can only handle addition of a new column at
 * the end. It extends DdlRebuildTableStmt since the default implementation for
 * the reshape is to rebuild the entire table by copying from the old structure
 * to the new.
 *
 * @author John Sichi
 * @version $Id$
 */
public class DdlAlterTableStructureStmt
    extends DdlReloadTableStmt
{
    //~ Instance fields --------------------------------------------------------

    private CwmTable origTable;

    //~ Constructors -----------------------------------------------------------

    public DdlAlterTableStructureStmt(
        FarragoRepos repos,
        CwmTable table)
    {
        super(table);

        JmiModelGraph transientModelGraph =
            new JmiModelGraph(
                repos.getTransientFarragoPackage());

        // NOTE jvs 4-Dec-2008: Take a copy of the old table structure so that
        // we have it available for knowing how to access old stored tuples
        // while reshaping them.  We use the transient repository for this
        // purpose since we don't have versioning support in the persistent
        // repository.

        origTable =
            (CwmTable) cloneRefObj(
                repos.getModelGraph(),
                transientModelGraph,
                table);
        for (CwmFeature feature : table.getFeature()) {
            CwmColumn column = (CwmColumn) feature;
            CwmColumn origColumn =
                (CwmColumn) cloneRefObj(
                    repos.getModelGraph(),
                    transientModelGraph,
                    column);
            origColumn.setType(column.getType());
            origTable.getFeature().add(origColumn);
        }
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Clones an object from one repository to another, sort of.
     *
     * <p>TODO jvs 4-Dec-2008: make this handle more cases generically and then
     * promote it to JmiObjUtil.
     *
     * @param oldModelGraph model graph for the old repository
     * @param newModelGraph model graph for the new repository
     * @param oldObj object to be cloned
     *
     * @return result of cloning
     */
    private RefObject cloneRefObj(
        JmiModelGraph oldModelGraph,
        JmiModelGraph newModelGraph,
        RefObject oldObj)
    {
        JmiClassVertex oldClassVertex =
            oldModelGraph.getVertexForRefClass(oldObj.refClass());
        JmiClassVertex newClassVertex =
            newModelGraph.getVertexForClassName(
                oldClassVertex.getMofClass().getName());
        RefObject newObj =
            newClassVertex.getRefClass().refCreateInstance(
                Collections.EMPTY_LIST);
        SortedMap<String, Object> map = JmiObjUtil.getAttributeValues(oldObj);

        // The composition for default values causes problems, and
        // we don't actually need them, so toss 'em.
        map.remove("initialValue");
        JmiObjUtil.setAttributeValues(newObj, map);
        return newObj;
    }

    // override DdlReloadTableStmt
    public void prepForExecuteUnlocked(
        FarragoSessionDdlValidator ddlValidator,
        FarragoSession session)
    {
        super.prepForExecuteUnlocked(ddlValidator, session);
        FemRecoveryReference recoveryRef =
            FarragoCatalogUtil.createRecoveryReference(
                session.getRepos(),
                RecoveryTypeEnum.ALTER_TABLE_ADD_COLUMN,
                getTable());
        setRecoveryRef(recoveryRef);
    }

    // implement DdlReloadTableStmt
    protected CwmTable getOldTableStructureForIndexMap()
    {
        return origTable;
    }

    // implement DdlStmt
    public void preValidate(FarragoSessionDdlValidator ddlValidator)
    {
        if (getTable() instanceof FemForeignTable) {
            throw FarragoResource.instance().ValidatorAlterForeignTable.ex();
        }
        FemLocalTable localTable = (FemLocalTable) getTable();
        if (localTable.isTemporary()) {
            // TODO jvs 10-Dec-2008:  According to SQL:2003, only local
            // temporary tables should be prohibited, but thinking
            // about what it would take to make ALTER TABLE ADD COLUMN
            // work for global temporary tables is making my head hurt,
            // so for now I'm disabling it for all temporary tables.
            throw FarragoResource.instance().ValidatorAlterTempTable.ex();
        }
        FarragoDataWrapperCache wrapperCache =
            ddlValidator.getDataWrapperCache();
        FemDataServer femDataServer = localTable.getServer();
        FarragoMedLocalDataServer medDataServer =
            (FarragoMedLocalDataServer) wrapperCache.loadServerFromCatalog(
                femDataServer);
        if (!medDataServer.supportsAlterTableAddColumn()) {
            throw FarragoResource.instance().ValidatorAlterTableDataServer.ex(
                ddlValidator.getRepos().getLocalizedObjectName(femDataServer));
        }
    }

    // override DdlReloadTableStmt
    protected boolean shouldRebuildIndexes(
        FarragoSessionDdlValidator ddlValidator)
    {
        FarragoSessionPersonality personality =
            ddlValidator.getInvokingSession().getPersonality();
        if (personality.isAlterTableAddColumnIncremental()) {
            // no need to rebuild indexes
            return false;
        } else {
            return true;
        }
    }

    /**
     * Recovers after a problem (either execution failure or system crash) while
     * a table was being altered.
     *
     * @param repos repository containing table definition
     * @param failedTable table to recover
     */
    public static void recover(
        FarragoRepos repos,
        CwmTable failedTable)
    {
        // We need to delete the newly added column and any associated objects.
        // For now we can assume the new column is always at the end.
        FemStoredColumn column =
            (FemStoredColumn) failedTable.getFeature().remove(
                failedTable.getFeature().size() - 1);

        // For identity columns, we have to deal with the sequence
        // explicitly; it will get deleted automatically via
        // composition, but we need to take care of grants on
        // the sequence too, and those don't go away automatically.
        FemSequenceGenerator sequence = column.getSequence();
        if (sequence != null) {
            deleteGrants(repos, sequence);
        }

        CwmClassifier type = column.getType();
        if (type instanceof FemSqlcollectionType) {
            // For multisets and arrays, an anonymous type instance
            // is owned by the column, so we need to get rid of it;
            // see comments regarding DROP COLUMN in TypedElement
            // production rule of CommonDdlParser.jj.
            column.setType(null);
            deleteGrants(repos, type.getFeature().get(0));
            deleteModelElement(repos, type);
        }

        // If the column is part of a constraint, we can assume
        // that the constraint was newly added along with the column,
        // in which case it needs to go.  (We don't actually support
        // the DDL for this yet, but this code is intended to cover
        // that case once we do start to support it.)
        KeysIndexesPackage kiPkg = repos.getKeysIndexesPackage();
        Set<CwmUniqueKey> keys = new HashSet<CwmUniqueKey>();
        keys.addAll(
            kiPkg.getUniqueFeature().getUniqueKey(column));
        for (CwmUniqueKey key : keys) {
            FemAbstractKeyConstraint constraint =
                (FemAbstractKeyConstraint) key;
            for (FemKeyComponent component : constraint.getComponent()) {
                deleteGrants(repos, component);
            }
            deleteModelElement(repos, key);
        }

        // Likewise for indexes; this is already needed for LCS,
        // where each new column gets a corresponding new clustered
        // index.  TODO jvs 9-Dec-2008:  clean up index storage,
        // maybe as part of FIXME in super.recoverFromFailure.
        Set<FemLocalIndex> indexes = new HashSet<FemLocalIndex>();
        for (
            CwmIndexedFeature indexedFeature
            : kiPkg.getIndexedFeatures().getIndexedFeature(column))
        {
            indexes.add((FemLocalIndex) indexedFeature.getIndex());
        }
        for (FemLocalIndex index : indexes) {
            for (CwmIndexedFeature indexedFeature : index.getIndexedFeature()) {
                deleteGrants(repos, indexedFeature);
            }
            deleteModelElement(repos, index);
        }

        // OK, now the column itself can finally go.
        deleteModelElement(repos, column);
    }

    private static void deleteModelElement(
        FarragoRepos repos,
        CwmModelElement element)
    {
        // Get rid of granted privileges, which will have been
        // created automatically on behalf of the new element's creator.
        deleteGrants(repos, element);
        element.refDelete();
    }

    // REVIEW jvs 9-Dec-2008: Either this wants to be promoted to somewhere
    // public like FarragoCatalogUtil, or else this entire cleanup operations
    // really wants to be done using standard triggers from DdlValidator.
    // We're special-casing here the more general case of ALTER TABLE DROP
    // COLUMN.
    private static void deleteGrants(
        FarragoRepos repos,
        CwmModelElement element)
    {
        PrivilegeIsGrantedOnElement privAssoc =
            repos.getSecurityPackage().getPrivilegeIsGrantedOnElement();
        List<FemGrant> grants =
            new ArrayList<FemGrant>(
                privAssoc.getPrivilege(element));
        for (FemGrant grant : grants) {
            grant.refDelete();
        }
    }

    protected void recoverFromFailure(
        FarragoSessionDdlValidator ddlValidator,
        FarragoSession session)
    {
        super.recoverFromFailure(ddlValidator, session);
        recover(session.getRepos(), getTable());
    }

    // override DdlReloadTableStmt
    public void completeAfterExecuteUnlocked(
        FarragoSessionDdlValidator ddlValidator,
        FarragoSession session,
        boolean success)
    {
        super.completeAfterExecuteUnlocked(ddlValidator, session, success);
        if (!success) {
            return;
        }

        // Reset the creation timestamp on the new column to the
        // end-of-statement time so that labels created while the ALTER
        // was in progress will not have the column visible.
        FemStoredColumn column =
            (FemStoredColumn) getTable().getFeature().get(
                getTable().getFeature().size() - 1);
        String timestamp = FarragoCatalogUtil.createTimestamp();
        column.setModificationTimestamp(timestamp);
        column.setCreationTimestamp(timestamp);
    }

    // implement DdlReloadTableStmt
    protected String getReloadDml(SqlPrettyWriter writer)
    {
        // Generate the reshaping query
        // "insert into T(old-T.*) select old-T.* from T"
        // which will automatically fill in default values for
        // the new column as old rows are copied.

        // NOTE jvs 7-Dec-2008: In the future, when we support GENERATED ALWAYS
        // AS (expression), there's going to be an issue in the case where
        // expression evaluation produces an exception for a deleted row.  It
        // will be necessary to avoid or recover from such exceptions and fill
        // in a null/default instead.  Also, expression evaluation will
        // create an assumption that row order is preserved (so that
        // the new generated values get associated with the correct existing
        // rows), which is currently true, but could be violated
        // by enabling a horizontal-parallel executor.

        CwmTable table = getTable();
        int n = table.getFeature().size() - 1;

        // We only know how to handle adding one column at the end,
        // so do some sanity checking to make sure no one tried to
        // pull a fast one on us.
        assert (n == origTable.getFeature().size());

        SqlIdentifier tableName = FarragoCatalogUtil.getQualifiedName(table);

        writer.print("insert into ");
        tableName.unparse(writer, 0, 0);
        SqlWriter.Frame frame = writer.startList("(", ")");
        for (int i = 0; i < n; ++i) {
            writer.sep(",");
            CwmFeature column = table.getFeature().get(i);
            assert (column.getName().equals(
                origTable.getFeature().get(i).getName()));
            writer.identifier(column.getName());
        }
        writer.endList(frame);

        writer.print(" select ");
        frame = writer.startList("", "");
        for (int i = 0; i < n; ++i) {
            writer.sep(",");
            CwmFeature column = table.getFeature().get(i);
            writer.identifier(column.getName());
        }
        writer.endList(frame);
        writer.print(" from ");
        tableName.unparse(writer, 0, 0);
        String sql = writer.toString();
        return sql;
    }
}

// End DdlAlterTableStructureStmt.java

/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 2004-2005 John V. Sichi
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

import org.eigenbase.util.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.type.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.query.*;
import net.sf.farrago.session.*;
import net.sf.farrago.type.*;
import net.sf.farrago.util.*;

import net.sf.farrago.cwm.core.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.cwm.relational.enumerations.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.fem.med.*;

import java.io.*;
import java.nio.charset.*;
import java.sql.*;
import java.util.*;

/**
 * DdlRelationalHandler defines DDL handler methods for standard relational
 * objects such as schemas, tables, indexes, and views.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class DdlRelationalHandler extends DdlHandler
{
    protected final DdlMedHandler medHandler;
    
    public DdlRelationalHandler(
        DdlMedHandler medHandler)
    {
        super(medHandler.getValidator());
        this.medHandler = medHandler;
    }

    // implement FarragoSessionDdlHandler
    public void validateDefinition(CwmCatalog catalog)
    {
        validator.validateUniqueNames(
            catalog,
            catalog.getOwnedElement(),
            true);
    }
    
    // implement FarragoSessionDdlHandler
    public void validateModification(CwmCatalog catalog)
    {
        validateDefinition(catalog);
    }
    
    // implement FarragoSessionDdlHandler
    public void validateDefinition(FemLocalSchema schema)
    {
        validator.validateUniqueNames(
            schema,
            schema.getOwnedElement(),
            true);
    }
    
    // implement FarragoSessionDdlHandler
    public void validateModification(FemLocalSchema schema)
    {
        validateDefinition(schema);
    }
    
    // implement FarragoSessionDdlHandler
    public void validateDefinition(FemLocalIndex index)
    {
        CwmTable table = FarragoCatalogUtil.getIndexTable(index);
        if (table.isTemporary()) {
            if (!validator.isCreatedObject(table)) {
                // REVIEW: support this?  What to do about instances of the
                // same temporary table in other sessions?
                throw validator.res.newValidatorIndexOnExistingTempTable(
                    repos.getLocalizedObjectName(index),
                    repos.getLocalizedObjectName(table));
            }
        }

        // TODO:  verify columns distinct, total width acceptable, and all
        // columns indexable types
        index.setSorted(true);
        if (index.getNamespace() != null) {
            assert (
                index.getNamespace().equals(
                    table.getNamespace()));
        } else {
            index.setNamespace(table.getNamespace());
        }

        index.setFilterCondition("TRUE");
    }
    
    // implement FarragoSessionDdlHandler
    public void validateModification(FemLocalIndex index)
    {
        // indexes are never modified after creation
        throw new AssertionError();
    }
    
    // implement FarragoSessionDdlHandler
    public void validateModification(FemBaseColumnSet columnSet)
    {
        if (!columnSet.getServer().getWrapper().isForeign()) {
            validateBaseColumnSet(columnSet);
        }
    }

    // implement FarragoSessionDdlHandler
    public void validateDefinition(FemLocalTable table)
    {
        validateLocalTable(table, true);
    }
    
    // implement FarragoSessionDdlHandler
    public void validateModification(FemLocalTable table)
    {
        validateLocalTable(table, false);
    }
    
    public void validateLocalTable(FemLocalTable table, boolean creation)
    {
        FemDataServer dataServer = table.getServer();
        FemDataWrapper dataWrapper = dataServer.getWrapper();
        if (dataWrapper.isForeign()) {
            throw validator.res.newValidatorLocalTableButForeignWrapper(
                repos.getLocalizedObjectName(table),
                repos.getLocalizedObjectName(dataWrapper));
        }

        validateAttributeSet(table);

        Collection indexes = FarragoCatalogUtil.getTableIndexes(repos, table);

        // NOTE:  don't need to validate index name uniqueness since indexes
        // live in same schema as table, so enforcement will take place at
        // schema level
        Iterator indexIter = indexes.iterator();
        int nClustered = 0;
        while (indexIter.hasNext()) {
            FemLocalIndex index = (FemLocalIndex) indexIter.next();
            if (index.isClustered()) {
                nClustered++;
            }
        }
        if (nClustered > 1) {
            throw validator.res.newValidatorDuplicateClusteredIndex(
                repos.getLocalizedObjectName(table));
        }

        FemPrimaryKeyConstraint primaryKey = null;
        Iterator constraintIter = table.getOwnedElement().iterator();
        while (constraintIter.hasNext()) {
            Object obj = constraintIter.next();
            if (!(obj instanceof FemAbstractUniqueConstraint)) {
                continue;
            }
            FemAbstractUniqueConstraint constraint =
                (FemAbstractUniqueConstraint) obj;
            if (constraint instanceof FemPrimaryKeyConstraint) {
                if (primaryKey != null) {
                    throw validator.res.newValidatorMultiplePrimaryKeys(
                        repos.getLocalizedObjectName(table));
                }
                primaryKey = (FemPrimaryKeyConstraint) constraint;
            }
            if (creation) {
                // Implement constraints via system-owned indexes.
                FemLocalIndex index =
                    createUniqueConstraintIndex(table, constraint);
                if ((constraint == primaryKey) && (nClustered == 0)) {
                    // If no clustered index was specified, make the primary
                    // key's index clustered.
                    index.setClustered(true);
                }
                // Create redundant metadata used by JDBC views
                createConstraintColumnMetadata(constraint);
            }
        }

        if (primaryKey == null) {
            // TODO:  This is not SQL-standard.  Fixing it requires the
            // introduction of a system-managed surrogate key.
            throw validator.res.newValidatorNoPrimaryKey(
                repos.getLocalizedObjectName(table));
        }

        // NOTE:  do this after PRIMARY KEY uniqueness validation to get a
        // better error message in the case of generated constraint names
        validator.validateUniqueNames(
            table,
            table.getOwnedElement(),
            false);

        if (creation) {
            medHandler.validateMedColumnSet(table);
        }
    }
    
    // implement FarragoSessionDdlHandler
    public void validateDefinition(CwmView view)
    {
        FarragoSession session = validator.newReentrantSession();

        try {
            validateViewImpl(session, view);
        } catch (FarragoUnvalidatedDependencyException ex) {
            // pass this one through
            throw ex;
        } catch (Throwable ex) {
            throw validator.res.newValidatorInvalidObjectDefinition(
                repos.getLocalizedObjectName(view), 
                ex);
        } finally {
            validator.releaseReentrantSession(session);
        }
    }

    private void validateViewImpl(
        FarragoSession session,
        CwmView view)
        throws Throwable
    {
        String sql = view.getQueryExpression().getBody();

        tracer.fine(sql);
        FarragoSessionAnalyzedSql analyzedSql;
        try {
            analyzedSql = session.analyzeSql(
                sql, validator.getTypeFactory(), null);
        } catch (Throwable ex) {
            throw adjustExceptionParserPosition(view, ex);
        }
        RelDataType rowType = analyzedSql.resultType;

        List columnList = view.getFeature();
        boolean implicitColumnNames = true;

        if (columnList.size() != 0) {
            implicitColumnNames = false;

            // number of explicitly specified columns needs to match the number
            // of columns produced by the query
            if (rowType.getFieldList().size() != columnList.size()) {
                throw validator.res.newValidatorViewColumnCountMismatch();
            }
        }

        if (analyzedSql.hasDynamicParams) {
            throw validator.res.newValidatorInvalidViewDynamicParam();
        }

        if (analyzedSql.hasTopLevelOrderBy) {
            throw validator.res.newValidatorInvalidViewOrderBy();
        }

        // Derive column information from result set metadata
        FarragoTypeFactory typeFactory = validator.getTypeFactory();
        RelDataTypeField [] fields = rowType.getFields();
        for (int i = 0; i < fields.length; ++i) {
            FemViewColumn column;
            if (implicitColumnNames) {
                column = repos.newFemViewColumn();
                columnList.add(column);
            } else {
                column = (FemViewColumn) columnList.get(i);
            }
            convertFieldToCwmColumn(fields[i], column);
            validateAttribute(column);
        }

        validator.validateUniqueNames(
            view,
            view.getFeature(),
            false);

        view.getQueryExpression().setBody(analyzedSql.canonicalString);

        validator.createDependency(
            view, analyzedSql.dependencies);
    }

    public FemLocalIndex createUniqueConstraintIndex(
        FemLocalTable table, 
        FemAbstractUniqueConstraint constraint)
    {
        // TODO:  make index SYSTEM-owned so that it can't be
        // dropped explicitly
        FemLocalIndex index = repos.newFemLocalIndex();
        FarragoCatalogUtil.generateConstraintIndexName(
            repos, constraint, index);
        repos.getKeysIndexesPackage().getIndexSpansClass().add(table, index);

        // REVIEW:  same as DDL; why is this necessary?
        index.setSpannedClass(table);
        index.setUnique(true);

        int iOrdinal = 0;
        Iterator columnIter = constraint.getFeature().iterator();
        while (columnIter.hasNext()) {
            CwmColumn column = (CwmColumn) columnIter.next();
            FemLocalIndexColumn indexColumn = repos.newFemLocalIndexColumn();
            indexColumn.setName(column.getName());
            indexColumn.setAscending(Boolean.TRUE);
            indexColumn.setFeature(column);
            indexColumn.setIndex(index);
            indexColumn.setOrdinal(iOrdinal++);
        }
        return index;
    }
    
    private void createConstraintColumnMetadata(
        FemAbstractUniqueConstraint constraint)
    {
        int iOrdinal = 0;
        Iterator columnIter = constraint.getFeature().iterator();
        while (columnIter.hasNext()) {
            FemAbstractAttribute column =
                (FemAbstractAttribute) columnIter.next();
            FemKeyComponent component = repos.newFemKeyComponent();
            component.setName(column.getName());
            component.setAttribute(column);
            constraint.getComponent().add(component);
            component.setOrdinal(iOrdinal++);
        }        
    }
    
    // implement FarragoSessionDdlHandler
    public void validateDrop(FemLocalIndex index)
    {
        CwmTable table = FarragoCatalogUtil.getIndexTable(index);
        if (validator.isDeletedObject(table)) {
            // This index is being deleted together with its containing table,
            // which is always OK.
            return;
        }

        if (index.isClustered()) {
            throw validator.newPositionalError(
                index, 
                validator.res.newValidatorDropClusteredIndex(
                    repos.getLocalizedObjectName(index)));
        }

        if (table.isTemporary()) {
            // REVIEW: support this?  What to do about instances of the
            // same temporary table in other sessions?
            throw validator.res.newValidatorIndexOnExistingTempTable(
                repos.getLocalizedObjectName(index),
                repos.getLocalizedObjectName(table));
        }
    }
    
    // implement FarragoSessionDdlHandler
    public void validateTruncation(FemLocalTable table)
    {
        Collection indexes = FarragoCatalogUtil.getTableIndexes(repos, table);
        Iterator indexIter = indexes.iterator();
        while (indexIter.hasNext()) {
            FemLocalIndex index = (FemLocalIndex) indexIter.next();
            validator.scheduleTruncation(index);
        }
    }
    
    // implement FarragoSessionDdlHandler
    public void executeCreation(FemLocalIndex index)
    {
        if (FarragoCatalogUtil.isIndexTemporary(index)) {
            // definition of a temporary table should't create any real storage
            return;
        }

        validator.getIndexMap().createIndexStorage(
            validator.getDataWrapperCache(), index);

        // TODO:  index existing rows; for now, creating an index on a
        // non-empty table will leave the index (incorrectly) empty
    }
    
    // implement FarragoSessionDdlHandler
    public void executeDrop(FemLocalIndex index)
    {
        // TODO: For a temporary table, need to drop storage for ALL sessions.
        // For now, storage from other sessions becomes garbage which will be
        // collected as those sessions close (or on recovery in case of a
        // crash).
        validator.getIndexMap().dropIndexStorage(
            validator.getDataWrapperCache(), index, false);
    }
    
    // implement FarragoSessionDdlHandler
    public void executeTruncation(FemLocalIndex index)
    {
        validator.getIndexMap().dropIndexStorage(
            validator.getDataWrapperCache(), index, true);
    }
}

// End DdlRelationalHandler.java

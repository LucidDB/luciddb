/*
// $Id$
// Farrago is a relational database management system.
// Copyright (C) 2004-2004 John V. Sichi.
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
package net.sf.farrago.ddl;

import org.eigenbase.util.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.sql.type.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.query.*;
import net.sf.farrago.session.*;
import net.sf.farrago.trace.*;
import net.sf.farrago.type.*;
import net.sf.farrago.util.*;
import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.util.*;

import net.sf.farrago.cwm.core.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.cwm.relational.enumerations.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fem.sql2003.*;

import java.io.*;
import java.nio.charset.*;
import java.sql.*;
import java.util.*;
import java.util.logging.*;

/**
 * DdlHandler provides implementations for the actions taken by
 * DdlValidator on individual objects.  See {@link FarragoSessionDdlHandler}
 * for an explanation.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class DdlHandler
{
    private static final Logger tracer = FarragoTrace.getDdlValidatorTracer();
    
    protected final DdlValidator validator;
    
    public DdlHandler(DdlValidator validator)
    {
        this.validator = validator;
    }
    
    public void validateDefinition(CwmCatalog catalog)
    {
        validator.validateUniqueNames(
            catalog,
            catalog.getOwnedElement(),
            true);
    }
    
    public void validateDefinition(CwmSchema schema)
    {
        validator.validateUniqueNames(
            schema,
            schema.getOwnedElement(),
            true);
    }
    
    public void validateDefinition(CwmSqlindex index)
    {
        CwmTable table = validator.getRepos().getIndexTable(index);
        if (table.isTemporary()) {
            if (!validator.isCreatedObject(table)) {
                // REVIEW: support this?  What to do about instances of the
                // same temporary table in other sessions?
                throw validator.res.newValidatorIndexOnExistingTempTable(
                    validator.getRepos().getLocalizedObjectName(index, null),
                    validator.getRepos().getLocalizedObjectName(
                        table,
                        null));
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
    
    public void validateModification(CwmSqlindex index)
    {
        // indexes are never modified after creation
        throw new AssertionError();
    }
    
    public void validateModification(FemBaseColumnSet columnSet)
    {
        if (!columnSet.getServer().getWrapper().isForeign()) {
            validateBaseColumnSet(columnSet);
        }
    }

    public void validateDefinition(FemForeignTable foreignTable)
    {
        validateForeignColumnSetDefinition(foreignTable);
    }
    
    protected void validateForeignColumnSetDefinition(
        FemBaseColumnSet columnSet)
    {
        FarragoRepos repos = validator.getRepos();
        FemDataServer dataServer = columnSet.getServer();
        FemDataWrapper dataWrapper = dataServer.getWrapper();

        if (!dataWrapper.isForeign()) {
            throw validator.res.newValidatorForeignTableButLocalWrapper(
                repos.getLocalizedObjectName(columnSet, null),
                repos.getLocalizedObjectName(dataWrapper, null));
        }
        
        validateBaseColumnSet(columnSet);

        FarragoMedColumnSet medColumnSet =
            validateMedColumnSet(columnSet);

        List columnList = columnSet.getFeature();
        if (columnList.isEmpty()) {
            // derive column information
            RelDataType rowType = medColumnSet.getRowType();
            RelDataTypeField [] fields = rowType.getFields();
            for (int i = 0; i < fields.length; ++i) {
                FemStoredColumn column = repos.newFemStoredColumn();
                columnList.add(column);
                convertFieldToCwmColumn(fields[i], column);
                validateColumnImpl(column);
            }
        }
    }
    
    public void validateDefinition(FemLocalTable table)
    {
        validateLocalTable(table, true);
    }
    
    public void validateModification(FemLocalTable table)
    {
        validateLocalTable(table, false);
    }
    
    protected void validateLocalTable(FemLocalTable table, boolean creation)
    {
        // need to validate columns first
        Iterator columnIter = table.getFeature().iterator();
        while (columnIter.hasNext()) {
            FemStoredColumn column = (FemStoredColumn) columnIter.next();
            validateLocalTableColumn(column);
        }

        FemDataServer dataServer = table.getServer();
        FemDataWrapper dataWrapper = dataServer.getWrapper();
        if (dataWrapper.isForeign()) {
            throw validator.res.newValidatorLocalTableButForeignWrapper(
                validator.getRepos().getLocalizedObjectName(table, null),
                validator.getRepos().getLocalizedObjectName(dataWrapper, null));
        }

        validator.validateUniqueNames(
            table,
            table.getFeature(),
            false);

        Collection indexes = validator.getRepos().getIndexes(table);

        // NOTE:  don't need to validate index name uniqueness since indexes
        // live in same schema as table, so enforcement will take place at
        // schema level
        Iterator indexIter = indexes.iterator();
        int nClustered = 0;
        while (indexIter.hasNext()) {
            CwmSqlindex index = (CwmSqlindex) indexIter.next();
            if (validator.getRepos().isClustered(index)) {
                nClustered++;
            }
        }
        if (nClustered > 1) {
            throw validator.res.newValidatorDuplicateClusteredIndex(
                table.getName());
        }

        CwmPrimaryKey primaryKey = null;
        Iterator constraintIter = table.getOwnedElement().iterator();
        while (constraintIter.hasNext()) {
            Object obj = constraintIter.next();
            if (!(obj instanceof CwmUniqueConstraint)) {
                continue;
            }
            CwmUniqueConstraint constraint = (CwmUniqueConstraint) obj;
            if (constraint instanceof CwmPrimaryKey) {
                if (primaryKey != null) {
                    throw validator.res.newValidatorMultiplePrimaryKeys(
                        table.getName());
                }
                primaryKey = (CwmPrimaryKey) constraint;
            }
            if (creation) {
                // Implement constraints via system-owned indexes.
                CwmSqlindex index =
                    createUniqueConstraintIndex(table, constraint);
                if ((constraint == primaryKey) && (nClustered == 0)) {
                    // If no clustered index was specified, make the primary
                    // key's index clustered.
                    validator.getRepos().setTagValue(
                        index,
                        "clusteredIndex",
                        "");
                }
            }
        }

        if (primaryKey == null) {
            // TODO:  This is not SQL-standard.  Fixing it requires the
            // introduction of a system-managed surrogate key.
            throw validator.res.newValidatorNoPrimaryKey(table.getName());
        }

        // NOTE:  do this after PRIMARY KEY uniqueness validation to get a
        // better error message in the case of generated constraint names
        validator.validateUniqueNames(
            table,
            table.getOwnedElement(),
            false);

        if (creation) {
            validateMedColumnSet(table);
        }
    }
    
    protected void validateBaseColumnSet(FemBaseColumnSet columnSet)
    {
        validator.validateUniqueNames(
            columnSet,
            columnSet.getFeature(),
            false);

        Iterator iter = columnSet.getFeature().iterator();
        while (iter.hasNext()) {
            FemStoredColumn column = (FemStoredColumn) iter.next();
            validateColumnImpl(column);
        }

        // Foreign tables should not support constraint definitions.  Eventually
        // we may want to allow this as a hint to the optimizer, but it's not
        // standard so for now we should prevent it.
        Iterator constraintIter = columnSet.getOwnedElement().iterator();
        while (constraintIter.hasNext()) {
            Object obj = constraintIter.next();
            if (!(obj instanceof CwmUniqueConstraint)) {
                continue;
            }
            throw validator.res.newValidatorNoConstraintAllowed(
                getLocalizedName(columnSet));
        }
    }

    protected FarragoMedColumnSet validateMedColumnSet(
        FemBaseColumnSet femColumnSet)
    {
        FarragoMedColumnSet medColumnSet;

        try {
            // validate that we can successfully initialize the table
            medColumnSet =
                validator.getDataWrapperCache().loadColumnSetFromCatalog(
                    femColumnSet,
                    validator.getTypeFactory());
        } catch (Throwable ex) {
            throw validator.res.newValidatorDataServerTableInvalid(
                validator.getRepos().getLocalizedObjectName(
                    femColumnSet,
                    femColumnSet.refClass()),
                ex);
        }

        validator.createDependency(
            femColumnSet,
            Collections.singleton(femColumnSet.getServer()),
            "ServerProvidesColumnSet");

        return medColumnSet;
    }
    
    protected String getLocalizedName(FemBaseColumnSet femBaseColumnSet)
    {
        return validator.getRepos().getLocalizedObjectName(
            null,
            femBaseColumnSet.getName(),
            femBaseColumnSet.refClass());
    }

    public void validateModification(CwmView view)
    {
        // nothing to do
    }
    
    public void validateDefinition(CwmView view)
    {
        FarragoSession session = validator.newReentrantSession();

        try {
            validateViewImpl(session, view);
        } catch (FarragoUnvalidatedDependencyException ex) {
            // pass this one through
            throw ex;
        } catch (Throwable ex) {
            // TODO: if ex has parser position information in it, need to
            // either delete it or adjust it
            throw validator.res.newValidatorInvalidViewDefinition(
                view.getName(),
                ex);
        } finally {
            validator.releaseReentrantSession(session);
        }
    }
    
    protected void validateViewImpl(
        FarragoSession session,
        CwmView view)
        throws SQLException
    {
        String sql = view.getQueryExpression().getBody();

        tracer.fine(sql);
        FarragoSessionViewInfo viewInfo = session.analyzeViewQuery(sql);
        ResultSetMetaData metaData = viewInfo.resultMetaData;

        List columnList = view.getFeature();
        boolean implicitColumnNames = true;

        if (columnList.size() != 0) {
            implicitColumnNames = false;

            // number of explicitly specified columns needs to match the number
            // of columns produced by the query
            if (metaData.getColumnCount() != columnList.size()) {
                throw validator.res.newValidatorViewColumnCountMismatch();
            }
        }

        if (viewInfo.parameterMetaData.getParameterCount() != 0) {
            throw validator.res.newValidatorInvalidViewDynamicParam();
        }

        // Derive column information from result set metadata
        FarragoTypeFactory typeFactory = validator.getTypeFactory();
        RelDataType rowType = typeFactory.createResultSetType(metaData);
        RelDataTypeField [] fields = rowType.getFields();
        for (int i = 0; i < fields.length; ++i) {
            FemViewColumn column;
            if (implicitColumnNames) {
                column = validator.getRepos().newFemViewColumn();
                columnList.add(column);
            } else {
                column = (FemViewColumn) columnList.get(i);
            }
            convertFieldToCwmColumn(fields[i], column);
            validateColumnImpl(column);
        }

        validator.validateUniqueNames(
            view,
            view.getFeature(),
            false);

        view.getQueryExpression().setBody(viewInfo.validatedSql);

        validator.createDependency(view, viewInfo.dependencies, "ViewUsage");
    }

    public void validateDefinition(CwmColumn column)
    {
        // let the containing table handle this by calling
        // validateColumn, since sometimes it has extra work to
        // do first
    }
    
    protected void validateLocalTableColumn(FemStoredColumn column)
    {
        validateColumnImpl(column);

        String defaultExpression = column.getInitialValue().getBody();
        if (!defaultExpression.equalsIgnoreCase("NULL")) {
            FarragoSession session = validator.newReentrantSession();
            try {
                validateDefaultClause(column, session, defaultExpression);
            } catch (Throwable ex) {
                throw validator.res.newValidatorBadDefaultClause(
                    column.getName(),
                    validator.getParserPosString(column),
                    ex);
            } finally {
                validator.releaseReentrantSession(session);
            }
        }
    }
    
    protected void validateDefaultClause(
        CwmColumn column,
        FarragoSession session,
        String defaultExpression)
    {
        String sql = "VALUES(" + defaultExpression + ")";
        FarragoSessionStmtContext stmtContext = session.newStmtContext();
        stmtContext.prepare(sql, false);
        RelDataType rowType = stmtContext.getPreparedRowType();
        assert (rowType.getFieldList().size() == 1);

        if (stmtContext.getPreparedParamType().getFieldList().size() > 0) {
            throw validator.res.newValidatorBadDefaultParam(
                column.getName(),
                validator.getParserPosString(column));
        }

        // SQL standard is very picky about what can go in a DEFAULT clause
        RelDataType sourceType = rowType.getFields()[0].getType();
        RelDataTypeFamily sourceTypeFamily = sourceType.getFamily();

        RelDataType targetType =
            validator.getTypeFactory().createColumnType(column);
        RelDataTypeFamily targetTypeFamily = targetType.getFamily();

        if (sourceTypeFamily != targetTypeFamily) {
            throw validator.res.newValidatorBadDefaultType(
                column.getName(),
                targetTypeFamily.toString(),
                sourceTypeFamily.toString(),
                validator.getParserPosString(column));
        }

        // TODO:  additional rules from standard, like no truncation allowed.
        // Maybe just execute SELECT with and without cast to target type and
        // make sure the same value comes back.
    }

    protected void validateColumnImpl(FemAbstractColumn column)
    {
        int ordinal = column.getOwner().getFeature().indexOf(column);
        assert (ordinal != -1);
        column.setOrdinal(ordinal);

        if (column.getInitialValue() == null) {
            CwmExpression nullExpression =
                validator.getRepos().newCwmExpression();
            nullExpression.setLanguage("SQL");
            nullExpression.setBody("NULL");
            column.setInitialValue(nullExpression);
        }

        // if NOT NULL not specified, default to nullable
        if (column.getIsNullable() == null) {
            column.setIsNullable(NullableTypeEnum.COLUMN_NULLABLE);
        }

        SqlTypeName typeName = SqlTypeName.get(
            column.getType().getName());

        // NOTE: parser only generates precision, but CWM discriminates
        // precision from length, so we take care of it below
        Integer precision = column.getPrecision();
        if (precision == null) {
            precision = column.getLength();
        }

        // TODO:  break this method up
        // first, validate presence of modifiers
        if ((typeName != null) && typeName.allowsPrec()) {
            if (precision == null) {
                int p = typeName.getDefaultPrecision();
                if (p != -1) {
                    precision = new Integer(p);
                }
            }
            if ((precision == null) && !typeName.allowsNoPrecNoScale()) {
                throw validator.res.newValidatorPrecRequired(
                    getLocalizedTypeName(column),
                    getLocalizedName(column),
                    validator.getParserPosString(column));
            }
        } else {
            if (precision != null) {
                throw validator.res.newValidatorPrecUnexpected(
                    getLocalizedTypeName(column),
                    getLocalizedName(column),
                    validator.getParserPosString(column));
            }
        }
        if ((typeName != null) && typeName.allowsScale()) {
            // assume scale is always optional
        } else {
            if (column.getScale() != null) {
                throw validator.res.newValidatorScaleUnexpected(
                    getLocalizedTypeName(column),
                    getLocalizedName(column),
                    validator.getParserPosString(column));
            }
        }
        SqlTypeFamily typeFamily = null;
        if (typeName != null) {
            typeFamily = SqlTypeFamily.getFamilyForSqlType(typeName);
        }
        if ((typeFamily == SqlTypeFamily.Character)
            || (typeFamily == SqlTypeFamily.Binary))
        {
            // convert precision to length
            if (column.getLength() == null) {
                column.setLength(column.getPrecision());
                column.setPrecision(null);
            }
        }
        if (typeFamily == SqlTypeFamily.Character) {
            // TODO jvs 18-April-2004:  Should be inheriting these defaults
            // from schema/catalog.
            if (JmiUtil.isBlank(column.getCharacterSetName())) {
                // NOTE: don't leave character set name implicit, since if the
                // default ever changed, that would invalidate existing data
                column.setCharacterSetName(
                    validator.getRepos().getDefaultCharsetName());
            } else {
                if (!Charset.isSupported(column.getCharacterSetName())) {
                    throw validator.res.newValidatorCharsetUnsupported(
                        column.getCharacterSetName(),
                        getLocalizedName(column),
                        validator.getParserPosString(column));
                }
            }
            Charset charSet = Charset.forName(column.getCharacterSetName());
            if (charSet.newEncoder().maxBytesPerChar() > 1) {
                // TODO:  implement multi-byte character sets
                throw Util.needToImplement(charSet);
            }
        } else {
            if (!JmiUtil.isBlank(column.getCharacterSetName())) {
                throw validator.res.newValidatorCharsetUnexpected(
                    getLocalizedTypeName(column),
                    getLocalizedName(column),
                    validator.getParserPosString(column));
            }
        }

        // now, enforce type-defined limits
        CwmSqldataType cwmType = validator.getStmtValidator().findSqldataType(
            column.getType().getName());

        // TODO jvs 15-Dec-2004:  non-simple types
        assert(cwmType instanceof CwmSqlsimpleType) : cwmType;
        CwmSqlsimpleType simpleType = (CwmSqlsimpleType) cwmType;
        
        if (column.getLength() != null) {
            Integer maximum = simpleType.getCharacterMaximumLength();
            assert (maximum != null);
            if (column.getLength().intValue() > maximum.intValue()) {
                throw validator.res.newValidatorLengthExceeded(
                    column.getLength(),
                    maximum,
                    getLocalizedName(column),
                    validator.getParserPosString(column));
            }
        }
        if (column.getPrecision() != null) {
            Integer maximum = simpleType.getNumericPrecision();
            if (maximum == null) {
                maximum = simpleType.getDateTimePrecision();
            }
            assert (maximum != null);
            if (column.getPrecision().intValue() > maximum.intValue()) {
                throw validator.res.newValidatorPrecisionExceeded(
                    column.getPrecision(),
                    maximum,
                    getLocalizedName(column),
                    validator.getParserPosString(column));
            }
        }
        if (column.getScale() != null) {
            Integer maximum = simpleType.getNumericScale();
            assert (maximum != null);
            if (column.getScale().intValue() > maximum.intValue()) {
                throw validator.res.newValidatorScaleExceeded(
                    column.getScale(),
                    maximum,
                    getLocalizedName(column),
                    validator.getParserPosString(column));
            }
        }

        // REVIEW jvs 18-April-2004: I had to put these in because CWM declares
        // them as mandatory.  This is stupid, since CWM also says these fields
        // are inapplicable for non-character columns.
        if (column.getCollationName() == null) {
            column.setCollationName("");
        }

        if (column.getCharacterSetName() == null) {
            column.setCharacterSetName("");
        }
    }

    public void validateDefinition(FemDataServer femServer)
    {
        FarragoRepos repos = validator.getRepos();

        // since servers are in the same namespace with CWM catalogs,
        // need a special name uniquness check here
        validator.validateUniqueNames(
            repos.getCwmCatalog(FarragoRepos.SYSBOOT_CATALOG_NAME),
            repos.relationalPackage.getCwmCatalog().refAllOfType(),
            false);

        try {
            // validate that we can successfully initialize the server
            validator.getDataWrapperCache().loadServerFromCatalog(femServer);
        } catch (Throwable ex) {
            throw validator.res.newValidatorDataServerInvalid(
                repos.getLocalizedObjectName(femServer, null),
                ex);
        }

        // REVIEW jvs 18-April-2004:  This uses default charset/collation
        // info from local catalog, but should really allow foreign
        // servers to override.
        repos.initializeCwmCatalog(femServer);

        // REVIEW jvs 18-April-2004:  Query the plugin for these?
        if (femServer.getType() == null) {
            femServer.setType("UNKNOWN");
        }
        if (femServer.getVersion() == null) {
            femServer.setVersion("UNKNOWN");
        }

        validator.createDependency(
            femServer,
            Collections.singleton(femServer.getWrapper()),
            "WrapperAccessesServer");
    }
    
    public void validateDefinition(FemDataWrapper femWrapper)
    {
        FarragoRepos repos = validator.getRepos();

        FarragoMedDataWrapper wrapper;
        try {
            if (!femWrapper.getLibraryFile().startsWith(
                    FarragoDataWrapperCache.LIBRARY_CLASS_PREFIX))
            {
                // convert library filename to absolute path, if necessary
                String libraryFile = femWrapper.getLibraryFile();

                String expandedLibraryFile =
                    FarragoProperties.instance().expandProperties(libraryFile);

                // REVIEW: SZ: 7/20/2004: Maybe the library should
                // always be an absolute path?  (e.g. Always report an
                // error if the path given by the user is relative.)
                // If a user installs a thirdparty Data Wrapper we
                // probably don't want them using relative paths to
                // call out its location.
                if (libraryFile.equals(expandedLibraryFile)) {
                    // No properties were expanded, so make the path
                    // absolute if it isn't already absolute.
                    File file = new File(libraryFile);
                    femWrapper.setLibraryFile(file.getAbsolutePath());
                } else {
                    // Test that the expanded library file is an
                    // absolute path.  We don't set the absolute path
                    // because we want to keep the property in the
                    // library name.
                    File file = new File(expandedLibraryFile);
                    if (!file.isAbsolute()) {
                        throw new IOException(libraryFile
                            + " does not evaluate to an absolute path");
                    }
                }
            }

            // validate that we can successfully initialize the wrapper
            wrapper = validator.getDataWrapperCache().loadWrapperFromCatalog(
                femWrapper);
        } catch (Throwable ex) {
            throw validator.res.newValidatorDataWrapperInvalid(
                repos.getLocalizedObjectName(femWrapper, null),
                ex);
        }

        if (femWrapper.isForeign()) {
            if (!wrapper.isForeign()) {
                throw validator.res.newValidatorForeignWrapperHasLocalImpl(
                    repos.getLocalizedObjectName(femWrapper, null));
            }
        } else {
            if (wrapper.isForeign()) {
                throw validator.res.newValidatorLocalWrapperHasForeignImpl(
                    repos.getLocalizedObjectName(femWrapper, null));
            }
        }
    }

    protected String getLocalizedName(CwmColumn column)
    {
        return validator.getRepos().getLocalizedObjectName(
            null,
            column.getName(),
            column.refClass());
    }

    protected String getLocalizedTypeName(CwmColumn column)
    {
        return validator.getRepos().getLocalizedObjectName(
            column.getType(),
            column.getType().refClass());
    }
    
    protected CwmSqlindex createUniqueConstraintIndex(
        FemLocalTable table, 
        CwmUniqueConstraint constraint)
    {
        // TODO:  make index SYSTEM-owned so that it can't be
        // dropped explicitly
        FarragoRepos repos = validator.getRepos();
        CwmSqlindex index = repos.newCwmSqlindex();
        repos.generateConstraintIndexName(constraint, index);
        repos.indexPackage.getIndexSpansClass().add(table, index);

        // REVIEW:  same as DDL; why is this necessary?
        index.setSpannedClass(table);
        index.setUnique(true);

        Iterator columnIter = constraint.getFeature().iterator();
        while (columnIter.hasNext()) {
            CwmColumn column = (CwmColumn) columnIter.next();
            CwmSqlindexColumn indexColumn = repos.newCwmSqlindexColumn();
            indexColumn.setName(column.getName());
            indexColumn.setAscending(Boolean.TRUE);
            indexColumn.setFeature(column);
            indexColumn.setIndex(index);
        }
        return index;
    }
    
    public void validateDrop(CwmSqlindex index)
    {
        CwmTable table = validator.getRepos().getIndexTable(index);
        if (validator.isDeletedObject(table)) {
            // This index is being deleted together with its containing table,
            // which is always OK.
            return;
        }

        if (validator.getRepos().isClustered(index)) {
            throw validator.res.newValidatorDropClusteredIndex(
                validator.getRepos().getLocalizedObjectName(index, null),
                validator.getParserPosString(index));
        }

        if (table.isTemporary()) {
            // REVIEW: support this?  What to do about instances of the
            // same temporary table in other sessions?
            throw validator.res.newValidatorIndexOnExistingTempTable(
                validator.getRepos().getLocalizedObjectName(index, null),
                validator.getRepos().getLocalizedObjectName(
                    table,
                    null));
        }
    }
    
    public void validateTruncation(FemLocalTable table)
    {
        Collection indexes = validator.getRepos().getIndexes(table);
        Iterator indexIter = indexes.iterator();
        while (indexIter.hasNext()) {
            CwmSqlindex index = (CwmSqlindex) indexIter.next();
            validator.scheduleTruncation(index);
        }
    }
    
    public void executeCreation(CwmSqlindex index)
    {
        if (validator.getRepos().getIndexTable(index).isTemporary()) {
            // definition of a temporary table should't create any real storage
            return;
        }

        validator.getIndexMap().createIndexStorage(
            validator.getDataWrapperCache(), index);

        // TODO:  index existing rows; for now, creating an index on a
        // non-empty table will leave the index (incorrectly) empty
    }
    
    public void executeDrop(CwmSqlindex index)
    {
        // TODO: For a temporary table, need to drop storage for ALL sessions.
        // For now, storage from other sessions becomes garbage which will be
        // collected as those sessions close (or on recovery in case of a
        // crash).
        validator.getIndexMap().dropIndexStorage(
            validator.getDataWrapperCache(), index, false);
    }
    
    public void executeTruncation(CwmSqlindex index)
    {
        validator.getIndexMap().dropIndexStorage(
            validator.getDataWrapperCache(), index, true);
    }
    
    public void executeDrop(FemDataServer server)
    {
        validator.discardDataWrapper(server);
    }

    public void executeDrop(FemDataWrapper wrapper)
    {
        validator.discardDataWrapper(wrapper);
    }
    
    /**
     * Initializes a CwmColumn definition based on a RelDataTypeField.
     * If the column has no name, the name is initialized from the field
     * name; otherwise, the existing name is left unmodified.
     *
     * @param field input field
     *
     * @param column on input, contains unintialized CwmColumn instance;
     * on return, this has been initialized (but not validated)
     */
    private void convertFieldToCwmColumn(
        RelDataTypeField field,
        CwmColumn column)
    {
        RelDataType type = field.getType();
        if (column.getName() == null) {
            column.setName(field.getName());
        }
        SqlTypeName typeName = type.getSqlTypeName();
        String lookupName;
        if (typeName == null) {
            // TODO jvs 15-Dec-2005:  UDF etc
            lookupName = type.toString();
        } else {
            lookupName = typeName.getName();
        }
        CwmSqldataType cwmType = 
            validator.getStmtValidator().findSqldataType(lookupName);
        column.setType(cwmType);
        if (typeName != null) {
            if (typeName.allowsPrec()) {
                column.setPrecision(new Integer(type.getPrecision()));
                if (typeName.allowsScale()) {
                    column.setScale(new Integer(type.getScale()));
                }
            }
        }
        if (type.isNullable()) {
            column.setIsNullable(NullableTypeEnum.COLUMN_NULLABLE);
        } else {
            column.setIsNullable(NullableTypeEnum.COLUMN_NO_NULLS);
        }
    }
}

// End DdlHandler.java

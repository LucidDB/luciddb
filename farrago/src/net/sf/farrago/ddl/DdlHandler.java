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
import org.eigenbase.sql.*;
import org.eigenbase.sql.type.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.query.*;
import net.sf.farrago.session.*;
import net.sf.farrago.trace.*;
import net.sf.farrago.type.*;
import net.sf.farrago.util.*;
import net.sf.farrago.plugin.*;
import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.util.*;

import net.sf.farrago.cwm.core.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.cwm.relational.enumerations.*;
import net.sf.farrago.cwm.behavioral.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fem.sql2003.*;

import java.io.*;
import java.net.*;
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

    protected final FarragoRepos repos;
    
    public DdlHandler(DdlValidator validator)
    {
        this.validator = validator;
        repos = validator.getRepos();
    }
    
    public void validateDefinition(CwmCatalog catalog)
    {
        validator.validateUniqueNames(
            catalog,
            catalog.getOwnedElement(),
            true);
    }
    
    public void validateDefinition(FemLocalSchema schema)
    {
        validator.validateUniqueNames(
            schema,
            schema.getOwnedElement(),
            true);
    }
    
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
    
    public void validateModification(FemLocalIndex index)
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
        FemDataServer dataServer = columnSet.getServer();
        FemDataWrapper dataWrapper = dataServer.getWrapper();

        if (!dataWrapper.isForeign()) {
            throw validator.res.newValidatorForeignTableButLocalWrapper(
                repos.getLocalizedObjectName(columnSet),
                repos.getLocalizedObjectName(dataWrapper));
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
                repos.getLocalizedObjectName(table),
                repos.getLocalizedObjectName(dataWrapper));
        }

        validator.validateUniqueNames(
            table,
            table.getFeature(),
            false);

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
                        repos.getLocalizedObjectName(table));
                }
                primaryKey = (CwmPrimaryKey) constraint;
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
                repos.getLocalizedObjectName(columnSet));
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
                repos.getLocalizedObjectName(femColumnSet),
                ex);
        }

        validator.createDependency(
            femColumnSet,
            Collections.singleton(femColumnSet.getServer()),
            "ServerProvidesColumnSet");

        return medColumnSet;
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
            throw validator.res.newValidatorInvalidObjectDefinition(
                repos.getLocalizedObjectName(view), 
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
        FarragoSessionAnalyzedSql analyzedSql = session.analyzeSql(sql, null);
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
            throw FarragoResource.instance().newValidatorInvalidViewOrderBy();
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
            validateColumnImpl(column);
        }

        validator.validateUniqueNames(
            view,
            view.getFeature(),
            false);

        view.getQueryExpression().setBody(analyzedSql.canonicalString);

        validator.createDependency(
            view, analyzedSql.dependencies, "ViewUsage");
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
                throw validator.newPositionalError(
                    column,
                    validator.res.newValidatorBadDefaultClause(
                        repos.getLocalizedObjectName(column), 
                        ex));
            } finally {
                validator.releaseReentrantSession(session);
            }
        }
    }
    
    protected void validateDefaultClause(
        FemStoredColumn column,
        FarragoSession session,
        String defaultExpression)
    {
        String sql = "VALUES(" + defaultExpression + ")";
        FarragoSessionStmtContext stmtContext = session.newStmtContext();
        stmtContext.prepare(sql, false);
        RelDataType rowType = stmtContext.getPreparedRowType();
        assert (rowType.getFieldList().size() == 1);

        if (stmtContext.getPreparedParamType().getFieldList().size() > 0) {
            throw validator.newPositionalError(
                column,
                validator.res.newValidatorBadDefaultParam(
                    repos.getLocalizedObjectName(column)));
        }

        // SQL standard is very picky about what can go in a DEFAULT clause
        RelDataType sourceType = rowType.getFields()[0].getType();
        RelDataTypeFamily sourceTypeFamily = sourceType.getFamily();

        RelDataType targetType =
            validator.getTypeFactory().createCwmElementType(column);
        RelDataTypeFamily targetTypeFamily = targetType.getFamily();

        if (sourceTypeFamily != targetTypeFamily) {
            throw validator.newPositionalError(
                column,
                validator.res.newValidatorBadDefaultType(
                    repos.getLocalizedObjectName(column),
                    targetTypeFamily.toString(),
                    sourceTypeFamily.toString()));
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

        validateTypedElement(column);
    }
    
    protected void validateTypedElement(FemSqltypedElement element)
    {
        if (element.getInitialValue() == null) {
            CwmExpression nullExpression = repos.newCwmExpression();
            nullExpression.setLanguage("SQL");
            nullExpression.setBody("NULL");
            element.setInitialValue(nullExpression);
        }

        // if NOT NULL not specified, default to nullable
        if (element.getIsNullable() == null) {
            element.setIsNullable(NullableTypeEnum.COLUMN_NULLABLE);
        }

        SqlTypeName typeName = SqlTypeName.get(
            element.getType().getName());

        // NOTE: parser only generates precision, but CWM discriminates
        // precision from length, so we take care of it below
        Integer precision = element.getPrecision();
        if (precision == null) {
            precision = element.getLength();
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
                throw validator.newPositionalError(
                    element,
                    validator.res.newValidatorPrecRequired(
                        repos.getLocalizedObjectName(element.getType()),
                        repos.getLocalizedObjectName(element)));
            }
        } else {
            if (precision != null) {
                throw validator.newPositionalError(
                    element,
                    validator.res.newValidatorPrecUnexpected(
                        repos.getLocalizedObjectName(element.getType()),
                        repos.getLocalizedObjectName(element)));
            }
        }
        if ((typeName != null) && typeName.allowsScale()) {
            // assume scale is always optional
        } else {
            if (element.getScale() != null) {
                throw validator.newPositionalError(
                    element,
                    validator.res.newValidatorScaleUnexpected(
                        repos.getLocalizedObjectName(element.getType()),
                        repos.getLocalizedObjectName(element)));
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
            if (element.getLength() == null) {
                element.setLength(element.getPrecision());
                element.setPrecision(null);
            }
        }
        if (typeFamily == SqlTypeFamily.Character) {
            // TODO jvs 18-April-2004:  Should be inheriting these defaults
            // from schema/catalog.
            if (JmiUtil.isBlank(element.getCharacterSetName())) {
                // NOTE: don't leave character set name implicit, since if the
                // default ever changed, that would invalidate existing data
                element.setCharacterSetName(
                    repos.getDefaultCharsetName());
            } else {
                if (!Charset.isSupported(element.getCharacterSetName())) {
                    throw validator.newPositionalError(
                        element,
                        validator.res.newValidatorCharsetUnsupported(
                            element.getCharacterSetName(),
                            repos.getLocalizedObjectName(element)));
                }
            }
            Charset charSet = Charset.forName(element.getCharacterSetName());
            if (charSet.newEncoder().maxBytesPerChar() > 1) {
                // TODO:  implement multi-byte character sets
                throw Util.needToImplement(charSet);
            }
        } else {
            if (!JmiUtil.isBlank(element.getCharacterSetName())) {
                throw validator.newPositionalError(
                    element,
                    validator.res.newValidatorCharsetUnexpected(
                        repos.getLocalizedObjectName(element.getType()),
                        repos.getLocalizedObjectName(element)));
            }
        }

        // now, enforce type-defined limits
        CwmSqldataType cwmType = validator.getStmtValidator().findSqldataType(
            element.getType().getName());

        if (cwmType instanceof CwmSqlsimpleType) {
            CwmSqlsimpleType simpleType = (CwmSqlsimpleType) cwmType;

            if (element.getLength() != null) {
                Integer maximum = simpleType.getCharacterMaximumLength();
                assert (maximum != null);
                if (element.getLength().intValue() > maximum.intValue()) {
                    throw validator.newPositionalError(
                        element,
                        validator.res.newValidatorLengthExceeded(
                            element.getLength(),
                            maximum,
                            repos.getLocalizedObjectName(element)));
                }
            }
            if (element.getPrecision() != null) {
                Integer maximum = simpleType.getNumericPrecision();
                if (maximum == null) {
                    maximum = simpleType.getDateTimePrecision();
                }
                assert (maximum != null);
                if (element.getPrecision().intValue() > maximum.intValue()) {
                    throw validator.newPositionalError(
                        element,
                        validator.res.newValidatorPrecisionExceeded(
                            element.getPrecision(),
                            maximum,
                            repos.getLocalizedObjectName(element)));
                }
            }
            if (element.getScale() != null) {
                Integer maximum = simpleType.getNumericScale();
                assert (maximum != null);
                if (element.getScale().intValue() > maximum.intValue()) {
                    throw validator.newPositionalError(
                        element,
                        validator.res.newValidatorScaleExceeded(
                            element.getScale(),
                            maximum,
                            repos.getLocalizedObjectName(element)));
                }
            }
        } else if (cwmType instanceof FemSqlcollectionType) {
            FemSqlcollectionType collectionType =
                (FemSqlcollectionType) element.getType();
            FemSqltypedElement componentType =
                collectionType.getComponentType();
            validateTypedElement(componentType);
        } else {
            Util.permAssert(false,
                "only simple and collection types are supported");
        }

        // REVIEW jvs 18-April-2004: I had to put these in because CWM
        // declares them as mandatory.  This is stupid, since CWM also says
        // these fields are inapplicable for non-character types.
        if (element.getCollationName() == null) {
            element.setCollationName("");
        }

        if (element.getCharacterSetName() == null) {
            element.setCharacterSetName("");
        }
    }

    public void validateDefinition(FemDataServer femServer)
    {
        // since servers are in the same namespace with CWM catalogs,
        // need a special name uniquness check here
        validator.validateUniqueNames(
            repos.getCatalog(FarragoRepos.SYSBOOT_CATALOG_NAME),
            repos.getRelationalPackage().getCwmCatalog().refAllOfType(),
            false);

        try {
            // validate that we can successfully initialize the server
            validator.getDataWrapperCache().loadServerFromCatalog(femServer);
        } catch (Throwable ex) {
            throw validator.res.newValidatorDefinitionInvalid(
                repos.getLocalizedObjectName(femServer),
                ex);
        }

        // REVIEW jvs 18-April-2004:  This uses default charset/collation
        // info from local catalog, but should really allow foreign
        // servers to override.
        repos.initializeCatalog(femServer);

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
            throw validator.res.newValidatorDefinitionInvalid(
                repos.getLocalizedObjectName(femWrapper),
                ex);
        }

        if (femWrapper.isForeign()) {
            if (!wrapper.isForeign()) {
                throw validator.res.newValidatorForeignWrapperHasLocalImpl(
                    repos.getLocalizedObjectName(femWrapper));
            }
        } else {
            if (wrapper.isForeign()) {
                throw validator.res.newValidatorLocalWrapperHasForeignImpl(
                    repos.getLocalizedObjectName(femWrapper));
            }
        }
    }

    public void validateDefinition(FemRoutine routine)
    {
        Iterator iter = routine.getParameter().iterator();
        int iOrdinal = 0;
        FemRoutineParameter returnParam = null;
        while (iter.hasNext()) {
            FemRoutineParameter param = (FemRoutineParameter) iter.next();
            if (param.getKind() == ParameterDirectionKindEnum.PDK_RETURN) {
                returnParam = param;
            } else {
                if (routine.getType().equals(ProcedureTypeEnum.FUNCTION)) {
                    if (param.getKind() != ParameterDirectionKindEnum.PDK_IN) {
                        throw validator.newPositionalError(
                            param,
                            validator.res.newValidatorFunctionOutputParam(
                                repos.getLocalizedObjectName(routine)));
                    }
                }
                param.setOrdinal(iOrdinal);
                ++iOrdinal;
            }
            validateRoutineParam(param);
        }
        if (routine.getDataAccess() == null) {
            throw validator.newPositionalError(
                routine,
                validator.res.newValidatorRoutineDataAccessUnspecified(
                    repos.getLocalizedObjectName(routine)));
        }

        if (routine.getBody() != null) {
            validateSqlRoutine(routine, returnParam);
        } else {
            validateJavaRoutine(routine, returnParam);
        }
    }

    private void validateSqlRoutine(
        FemRoutine routine,
        FemRoutineParameter returnParam)
    {
        if (routine.getLanguage() == null) {
            routine.setLanguage("SQL");
        }
        // TODO jvs 11-Jan-2005:  enum for supported languages
        if (!routine.getLanguage().equals("SQL")) {
            throw validator.newPositionalError(
                routine,
                validator.res.newValidatorRoutineBodySqlOnly(
                    repos.getLocalizedObjectName(routine)));
        }
        if (routine.getDataAccess() == RoutineDataAccessEnum.RDA_NO_SQL) {
            throw validator.newPositionalError(
                routine,
                validator.res.newValidatorRoutineNoSql(
                    repos.getLocalizedObjectName(routine)));
        }
        if (routine.getParameterStyle() == null) {
            routine.setParameterStyle(RoutineParameterStyleEnum.RPS_SQL);
        }
        if (routine.getParameterStyle() !=
            RoutineParameterStyleEnum.RPS_SQL)
        {
            throw validator.newPositionalError(
                routine,
                validator.res.newValidatorRoutineSqlParamStyleOnly(
                    repos.getLocalizedObjectName(routine)));
        }
        FarragoSession session = validator.newReentrantSession();
        try {
            validateRoutineBody(session, routine, returnParam);
        } catch (FarragoUnvalidatedDependencyException ex) {
            // pass this one through
            throw ex;
        } catch (Throwable ex) {
            // TODO: if ex has parser position information in it, need to
            // either delete it or adjust it
            throw validator.res.newValidatorInvalidObjectDefinition(
                repos.getLocalizedObjectName(routine), 
                ex);
        } finally {
            validator.releaseReentrantSession(session);
        }
    }

    private void validateJavaRoutine(
        FemRoutine routine, 
        FemRoutineParameter returnParam)
    {
        CwmProcedureExpression dummyBody =
            repos.newCwmProcedureExpression();
        dummyBody.setLanguage("JAVA");
        dummyBody.setBody(";");
        routine.setBody(dummyBody);
        
        if (routine.getLanguage() == null) {
            routine.setLanguage("JAVA");
        }
        if (!routine.getLanguage().equals("JAVA")) {
            throw validator.newPositionalError(
                routine,
                validator.res.newValidatorRoutineExternalJavaOnly(
                    repos.getLocalizedObjectName(routine)));
        }
        if (routine.getParameterStyle() == null) {
            routine.setParameterStyle(RoutineParameterStyleEnum.RPS_JAVA);
        }
        if (routine.getParameterStyle() != RoutineParameterStyleEnum.RPS_JAVA) {
            throw validator.newPositionalError(
                routine,
                validator.res.newValidatorRoutineJavaParamStyleOnly(
                    repos.getLocalizedObjectName(routine)));
        }

        FarragoUserDefinedRoutineLookup lookup =
            new FarragoUserDefinedRoutineLookup(
                validator.getStmtValidator(), null);
        FarragoUserDefinedRoutine sqlRoutine = lookup.convertRoutine(routine);
        try {
            sqlRoutine.getJavaMethod();
        } catch (SqlValidatorException ex) {
            throw validator.newPositionalError(routine,ex);
        }
        if (sqlRoutine.getJar() != null) {
            validator.createDependency(
                routine,
                Collections.singleton(sqlRoutine.getJar()),
                "RoutineUsesJar");
        }
    }

    protected void validateRoutineBody(
        FarragoSession session, 
        final FemRoutine routine,
        FemRoutineParameter returnParam)
        throws SQLException
    {
        final FarragoTypeFactory typeFactory = validator.getTypeFactory();
        final List params = routine.getParameter();

        RelDataType paramRowType = typeFactory.createStructType(
            new RelDataTypeFactory.FieldInfo() 
            {
                public int getFieldCount()
                {
                    return FarragoCatalogUtil.getRoutineParamCount(routine);
                }
                
                public String getFieldName(int index)
                {
                    FemRoutineParameter param =
                        (FemRoutineParameter) params.get(index);
                    return param.getName();
                }

                public RelDataType getFieldType(int index)
                {
                    FemRoutineParameter param =
                        (FemRoutineParameter) params.get(index);
                    return typeFactory.createCwmElementType(param);
                }
            });

        tracer.fine(routine.getBody().getBody());
        
        FarragoSessionAnalyzedSql analyzedSql = session.analyzeSql(
            routine.getBody().getBody(), paramRowType);
        
        if (analyzedSql.hasDynamicParams) {
            // TODO jvs 29-Dec-2004:  add a test for this; currently
            // hits an earlier assertion in SqlValidator
            throw validator.res.newValidatorInvalidRoutineDynamicParam();
        }

        // TODO jvs 28-Dec-2004:  CAST FROM
        
        RelDataType declaredReturnType =
            typeFactory.createCwmElementType(returnParam);
        RelDataType actualReturnType = analyzedSql.resultType;
        if (!SqlTypeUtil.canAssignFrom(declaredReturnType, actualReturnType)) {
            throw validator.res.newValidatorFunctionReturnType(
                actualReturnType.toString(),
                repos.getLocalizedObjectName(routine),
                declaredReturnType.toString());
        }

        validator.createDependency(
            routine, analyzedSql.dependencies, "RoutineUsage");
        
        routine.getBody().setBody(analyzedSql.canonicalString);
    }

    protected void validateRoutineParam(FemRoutineParameter param)
    {
        validateTypedElement(param);
    }

    protected FemLocalIndex createUniqueConstraintIndex(
        FemLocalTable table, 
        CwmUniqueConstraint constraint)
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
    
    public void validateTruncation(FemLocalTable table)
    {
        Collection indexes = FarragoCatalogUtil.getTableIndexes(repos, table);
        Iterator indexIter = indexes.iterator();
        while (indexIter.hasNext()) {
            FemLocalIndex index = (FemLocalIndex) indexIter.next();
            validator.scheduleTruncation(index);
        }
    }
    
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
    
    public void executeDrop(FemLocalIndex index)
    {
        // TODO: For a temporary table, need to drop storage for ALL sessions.
        // For now, storage from other sessions becomes garbage which will be
        // collected as those sessions close (or on recovery in case of a
        // crash).
        validator.getIndexMap().dropIndexStorage(
            validator.getDataWrapperCache(), index, false);
    }
    
    public void executeTruncation(FemLocalIndex index)
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
            // TODO jvs 15-Dec-2005:  UDT etc
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
    
    public void validateDefinition(FemJar jar)
    {
        // TODO jvs 19-Jan-2005: implement deployment descriptors, and
        // (optionally?) copy jar to an area managed by Farrago
        URL url;
        try {
            url = new URL(jar.getUrl());
        } catch (MalformedURLException ex) {
            throw validator.res.newPluginMalformedJarUrl(
                repos.getLocalizedObjectName(jar.getUrl()),
                repos.getLocalizedObjectName(jar),
                ex);
        }
    }
}

// End DdlHandler.java

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
package net.sf.farrago.query;

import java.nio.charset.*;

import java.util.*;

import javax.jmi.reflect.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.core.*;
import net.sf.farrago.cwm.datatypes.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.cwm.relational.enumerations.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.fennel.*;
import net.sf.farrago.namespace.util.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.session.*;
import net.sf.farrago.type.*;
import net.sf.farrago.util.*;

import org.eigenbase.jmi.*;
import org.eigenbase.resgen.*;
import org.eigenbase.resource.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.validate.*;
import org.eigenbase.trace.*;
import org.eigenbase.util.*;


/**
 * FarragoStmtValidator is a default implementation of the {@link
 * FarragoSessionStmtValidator} interface.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoStmtValidator
    extends FarragoCompoundAllocation
    implements FarragoSessionStmtValidator
{
    //~ Instance fields --------------------------------------------------------

    private final FarragoRepos repos;
    private final FennelDbHandle fennelDbHandle;
    private final FarragoSession session;
    private final FarragoTypeFactory typeFactory;
    private final FarragoSessionVariables sessionVariables;
    private final FarragoObjectCache codeCache;
    private final FarragoDataWrapperCache dataWrapperCache;
    private final FarragoSessionIndexMap indexMap;
    private final FarragoObjectCache sharedDataWrapperCache;
    private final FarragoSessionParser parser;
    private final FarragoSessionPrivilegeChecker privilegeChecker;
    private final FarragoDdlLockManager ddlLockManager;

    private SqlParserPos parserPos;
    private EigenbaseTimingTracer timingTracer;
    private FarragoReposTxnContext reposTxnContext;
    private FarragoWarningQueue warningQueue;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new FarragoStmtValidator object.
     *
     * @param repos repos to use for object definitions
     * @param fennelDbHandle handle to Fennel database to access
     * @param session invoking session
     * @param codeCache FarragoObjectCache to use for caching code snippets
     * needed during preparation
     * @param sharedDataWrapperCache FarragoObjectCache to use for caching
     * FarragoMedDataWrapper instances
     * @param ddlLockManager FarragoDdlLockManager to use for protecting catalog
     * objects in use from modification
     * @param indexMap FarragoSessionIndexMap to use for index access
     */
    public FarragoStmtValidator(
        FarragoRepos repos,
        FennelDbHandle fennelDbHandle,
        FarragoSession session,
        FarragoObjectCache codeCache,
        FarragoObjectCache sharedDataWrapperCache,
        FarragoSessionIndexMap indexMap,
        FarragoDdlLockManager ddlLockManager)
    {
        this.repos = repos;
        this.fennelDbHandle = fennelDbHandle;
        this.codeCache = codeCache;
        this.indexMap = indexMap;
        this.session = session;
        this.sharedDataWrapperCache = sharedDataWrapperCache;
        this.ddlLockManager = ddlLockManager;

        // default is a private warning queue, but normally
        // session resets queue to the one from stmt context
        this.warningQueue = new FarragoWarningQueue();

        parser = session.getPersonality().newParser(session);

        // clone session variables so that any context changes we make during
        // validation are transient
        sessionVariables = session.getSessionVariables().cloneVariables();
        typeFactory =
            (FarragoTypeFactory) session.getPersonality().newTypeFactory(repos);
        dataWrapperCache =
            new FarragoDataWrapperCache(
                this,
                sharedDataWrapperCache,
                session.getPluginClassLoader(),
                repos,
                fennelDbHandle,
                new FarragoSessionDataSource(session));
        privilegeChecker = session.newPrivilegeChecker();
    }

    //~ Methods ----------------------------------------------------------------

    // implement FarragoSessionStmtValidator
    public FarragoSessionParser getParser()
    {
        return parser;
    }

    // implement FarragoSessionStmtValidator
    public FarragoRepos getRepos()
    {
        return repos;
    }

    // implement FarragoSessionStmtValidator
    public FennelDbHandle getFennelDbHandle()
    {
        return fennelDbHandle;
    }

    // implement FarragoSessionStmtValidator
    public FarragoSession getSession()
    {
        return session;
    }

    // implement FarragoSessionStmtValidator
    public FarragoTypeFactory getTypeFactory()
    {
        return typeFactory;
    }

    // implement FarragoSessionStmtValidator
    public FarragoSessionVariables getSessionVariables()
    {
        return sessionVariables;
    }

    // implement FarragoSessionStmtValidator
    public FarragoObjectCache getCodeCache()
    {
        return codeCache;
    }

    // implement FarragoSessionStmtValidator
    public FarragoDataWrapperCache getDataWrapperCache()
    {
        return dataWrapperCache;
    }

    // implement FarragoSessionStmtValidator
    public FarragoSessionIndexMap getIndexMap()
    {
        return indexMap;
    }

    // implement FarragoSessionStmtValidator
    public FarragoObjectCache getSharedDataWrapperCache()
    {
        return sharedDataWrapperCache;
    }

    // implement FarragoSessionStmtValidator
    public FarragoSessionPrivilegeChecker getPrivilegeChecker()
    {
        return privilegeChecker;
    }

    // implement FarragoSessionStmtValidator
    public FarragoDdlLockManager getDdlLockManager()
    {
        return ddlLockManager;
    }

    // implement FarragoSessionStmtValidator
    public FarragoWarningQueue getWarningQueue()
    {
        return warningQueue;
    }

    // implement FarragoSessionStmtValidator
    public void setWarningQueue(FarragoWarningQueue warningQueue)
    {
        this.warningQueue = warningQueue;
    }

    // implement FarragoSessionStmtValidator
    public void requestPrivilege(
        CwmModelElement obj,
        String action)
    {
        // TODO jvs 27-Aug-2005:  cache user/role
        privilegeChecker.requestAccess(
            obj,
            FarragoCatalogUtil.getUserByName(
                getRepos(),
                sessionVariables.currentUserName),
            FarragoCatalogUtil.getRoleByName(
                getRepos(),
                sessionVariables.currentRoleName),
            action);
    }

    // implement FarragoSessionStmtValidator
    public CwmColumn findColumn(
        CwmNamedColumnSet namedColumnSet,
        String columnName)
    {
        CwmColumn column =
            (CwmColumn) FarragoCatalogUtil.getModelElementByName(
                namedColumnSet.getFeature(),
                columnName);
        if (column == null) {
            throw newPositionalError(
                FarragoResource.instance().ValidatorUnknownObjectInScope.ex(
                    getRepos().getLocalizedObjectName(
                        null,
                        columnName,
                        getRepos().getRelationalPackage().getCwmColumn()),
                    getRepos().getLocalizedObjectName(namedColumnSet)));
        }
        return column;
    }

    // implement FarragoSessionStmtValidator
    public CwmCatalog findCatalog(String catalogName)
    {
        return lookupCatalog(catalogName, true);
    }

    // implement FarragoSessionStmtValidator
    public SqlMoniker [] getAllSchemaObjectNames(String [] names)
    {
        // use default catalog.  If not set, don't do anything
        CwmCatalog catalog = repos.getCatalog(sessionVariables.catalogName);
        if (catalog == null) {
            return Util.emptySqlMonikerArray;
        }

        // look for both schema and object names
        if (names.length == 1) {
            // get all schema names
            List<SqlMonikerImpl> schemaNames =
                getAllObjectNamesByType(
                    catalog.getOwnedElement(),
                    FemLocalSchema.class);

            // if default schema is set, get all table names in this schema
            FemLocalSchema schema =
                FarragoCatalogUtil.getSchemaByName(
                    catalog,
                    sessionVariables.schemaName);
            if (schema != null) {
                List<SqlMonikerImpl> tableNames =
                    getAllObjectNamesByType(
                        schema.getOwnedElement(),
                        FemLocalTable.class);
                schemaNames.addAll(tableNames);
            }
            return schemaNames.toArray(Util.emptySqlMonikerArray);
        }
        // looking for table names under the specified schema
        else if (names.length == 2) {
            FemLocalSchema schema =
                FarragoCatalogUtil.getSchemaByName(
                    catalog,
                    names[0]);
            if (schema != null) {
                List<SqlMonikerImpl> tableNames =
                    getAllObjectNamesByType(
                        schema.getOwnedElement(),
                        FemLocalTable.class);
                return tableNames.toArray(Util.emptySqlMonikerArray);
            } else {
                return Util.emptySqlMonikerArray;
            }
        }
        // currently not supporting the likes of SALES.EMPS.$DUMMY
        else {
            return Util.emptySqlMonikerArray;
        }
    }

    /**
     * Returns a list of all object names of a given type in a collection.
     */
    private List<SqlMonikerImpl> getAllObjectNamesByType(
        Collection<CwmModelElement> collection,
        Class<? extends CwmModelElement> type)
    {
        List<SqlMonikerImpl> list = new ArrayList<SqlMonikerImpl>();
        for (CwmModelElement element : collection) {
            if (type.isInstance(element)) {
                // it only supports schema and table type right now
                if (element instanceof FemLocalSchema) {
                    list.add(
                        new SqlMonikerImpl(
                            element.getName(),
                            SqlMonikerType.Schema));
                } else if (element instanceof FemLocalTable) {
                    list.add(
                        new SqlMonikerImpl(
                            element.getName(),
                            SqlMonikerType.Table));
                }
            }
        }
        return list;
    }

    private CwmCatalog lookupCatalog(
        String catalogName,
        boolean throwIfNotFound)
    {
        CwmCatalog catalog = getRepos().getCatalog(catalogName);

        if ((catalog == null) && throwIfNotFound) {
            throw newPositionalError(
                FarragoResource.instance().ValidatorUnknownObject.ex(
                    getRepos().getLocalizedObjectName(
                        null,
                        catalogName,
                        getRepos().getRelationalPackage().getCwmCatalog())));
        }
        return catalog;
    }

    // implement FarragoSessionStmtValidator
    public CwmCatalog getDefaultCatalog()
    {
        return findCatalog(getSessionVariables().catalogName);
    }

    // implement FarragoSessionStmtValidator
    public FemLocalSchema findSchema(SqlIdentifier schemaName)
    {
        return lookupSchema(schemaName, true);
    }

    private FemLocalSchema lookupSchema(
        SqlIdentifier schemaName,
        boolean throwIfNotFound)
    {
        CwmCatalog catalog;
        String simpleName;
        if (schemaName.names.length == 2) {
            catalog = lookupCatalog(schemaName.names[0], throwIfNotFound);
            if (catalog == null) {
                return null;
            }
            simpleName = schemaName.names[1];
        } else {
            catalog =
                lookupCatalog(
                    getSessionVariables().catalogName,
                    throwIfNotFound);
            simpleName = schemaName.getSimple();
        }
        if (catalog == null) {
            return null;
        }
        FemLocalSchema schema =
            FarragoCatalogUtil.getSchemaByName(catalog, simpleName);

        // REVIEW:  parser context may be past schema name already
        if (schema == null) {
            if (!throwIfNotFound) {
                return null;
            }
            throw newPositionalError(
                FarragoResource.instance().ValidatorUnknownObject.ex(
                    getRepos().getLocalizedObjectName(
                        catalog.getName(),
                        simpleName,
                        getRepos().getRelationalPackage().getCwmSchema())));
        }
        return schema;
    }

    // implement FarragoSessionStmtValidator
    public FemDataWrapper findDataWrapper(
        SqlIdentifier wrapperName,
        boolean isForeign)
    {
        FemDataWrapper wrapper =
            FarragoCatalogUtil.getModelElementByName(
                getRepos().allOfType(FemDataWrapper.class),
                wrapperName.getSimple());
        if (wrapper != null) {
            if (wrapper.isForeign() != isForeign) {
                wrapper = null;
            }
        }
        if (wrapper == null) {
            throw newPositionalError(
                FarragoResource.instance().ValidatorUnknownObject.ex(
                    getRepos().getLocalizedObjectName(
                        null,
                        wrapperName.getSimple(),
                        getRepos().getMedPackage().getFemDataWrapper())));
        }
        return wrapper;
    }

    // implement FarragoSessionStmtValidator
    public FemDataServer findDataServer(SqlIdentifier serverName)
    {
        return findUnqualifiedObject(serverName, FemDataServer.class);
    }

    // implement FarragoSessionStmtValidator
    public FemDataServer getDefaultLocalDataServer()
    {
        String dataServerName =
            session.getPersonality().getDefaultLocalDataServerName(
                this);
        return findDataServer(
            new SqlIdentifier(dataServerName, SqlParserPos.ZERO));
    }

    // implement FarragoSessionStmtValidator
    public <T extends CwmModelElement> T findSchemaObject(
        SqlIdentifier qualifiedName,
        Class<T> clazz)
    {
        RefClass refClass = findRefClass(clazz);
        //        return clazz.cast(findSchemaObject(qualifiedName, refClass));

        FarragoSessionResolvedObject<T> resolved =
            resolveSchemaObjectName(qualifiedName.names, clazz);

        T element = null;
        String schemaName = null;

        if (resolved != null) {
            schemaName = resolved.schemaName;
            if (resolved.object != null) {
                element = resolved.object;
            }
        }

        if (element == null) {
            throw newPositionalError(
                FarragoResource.instance().ValidatorUnknownObject.ex(
                    getRepos().getLocalizedObjectName(
                        schemaName,
                        qualifiedName.names[qualifiedName.names.length - 1],
                        refClass)));
        }

        return element;
    }

    private RefClass findRefClass(Class<? extends RefObject> clazz)
    {
        JmiClassVertex vertex =
            repos.getModelGraph().getVertexForJavaInterface(clazz);
        assert vertex != null : "no vertex found for " + clazz;
        return vertex.getRefClass();
    }

    // implement FarragoSessionStmtValidator
    public <T extends CwmModelElement> T findUnqualifiedObject(
        SqlIdentifier unqualifiedName,
        Class<T> clazz)
    {
        RefClass refClass = findRefClass(clazz);
        T element =
            FarragoCatalogUtil.getModelElementByName(
                repos.allOfType(clazz),
                unqualifiedName.getSimple());
        if (element == null) {
            throw newPositionalError(
                FarragoResource.instance().ValidatorUnknownObject.ex(
                    getRepos().getLocalizedObjectName(
                        null,
                        unqualifiedName.getSimple(),
                        refClass)));
        }
        return element;
    }

    // implement FarragoSessionStmtValidator
    public List<FemRoutine> findRoutineOverloads(
        SqlIdentifier invocationName,
        ProcedureType routineType)
    {
        FarragoSessionVariables sessionVariables = getSessionVariables();
        Collection<CwmModelElement> routines;
        String simpleName;
        if (invocationName.names.length > 1) {
            // TODO jvs 19-Jan-2005: make this a utility method for extracting
            // schema name
            int nQualifiers = invocationName.names.length - 1;
            String [] schemaNames = new String[nQualifiers];
            System.arraycopy(
                invocationName.names,
                0,
                schemaNames,
                0,
                nQualifiers);
            simpleName = invocationName.names[nQualifiers];
            SqlIdentifier schemaId =
                new SqlIdentifier(schemaNames, SqlParserPos.ZERO);
            FemLocalSchema schema = findSchema(schemaId);
            routines = (Collection<CwmModelElement>) schema.getOwnedElement();
        } else {
            simpleName = invocationName.getSimple();

            // filter to only schemas on SQL-path
            routines = new ArrayList<CwmModelElement>();
            for (SqlIdentifier id : sessionVariables.schemaSearchPath) {
                CwmSchema searchedSchema = lookupSchema(id, false);
                if (searchedSchema == null) {
                    continue;
                }
                FarragoCatalogUtil.filterTypedModelElements(
                    (Collection<CwmModelElement>) searchedSchema
                    .getOwnedElement(),
                    routines,
                    FemRoutine.class);
            }
        }
        List<FemRoutine> overloads = new ArrayList<FemRoutine>();
        for (CwmModelElement element : routines) {
            if (!(element instanceof FemRoutine)) {
                continue;
            }
            FemRoutine routine = (FemRoutine) element;
            if ((routineType != null) && (routine.getType() != routineType)) {
                continue;
            }
            if (!routine.getInvocationName().equals(simpleName)) {
                continue;
            }
            overloads.add(routine);
        }
        return overloads;
    }

    // implement FarragoSessionStmtValidator
    public CwmSqldataType findSqldataType(SqlIdentifier typeName)
    {
        if (!typeName.isSimple()) {
            FemUserDefinedType udt =
                findSchemaObject(
                    typeName,
                    FemUserDefinedType.class);
            checkValidated(udt);
            return udt;
        }

        String simpleName = typeName.getSimple();

        Collection<CwmSqlsimpleType> types =
            getRepos().allOfClass(CwmSqlsimpleType.class);
        CwmSqlsimpleType simpleType =
            FarragoCatalogUtil.getModelElementByName(types, simpleName);
        if (simpleType != null) {
            return simpleType;
        }

        Collection<CwmTypeAlias> typeAliases =
            getRepos().allOfClass(CwmTypeAlias.class);
        CwmTypeAlias alias =
            FarragoCatalogUtil.getModelElementByName(
                typeAliases,
                simpleName);
        if (alias != null) {
            return (CwmSqldataType) alias.getType();
        }

        for (SqlIdentifier id : sessionVariables.schemaSearchPath) {
            CwmSchema searchedSchema = lookupSchema(id, false);
            if (searchedSchema == null) {
                continue;
            }
            FemUserDefinedType udt =
                FarragoCatalogUtil.getModelElementByNameAndType(
                    searchedSchema.getOwnedElement(),
                    simpleName,
                    FemUserDefinedType.class);
            if (udt != null) {
                checkValidated(udt);
                return udt;
            }
        }

        throw newPositionalError(
            FarragoResource.instance().ValidatorUnknownObject.ex(
                getRepos().getLocalizedObjectName(
                    null,
                    simpleName,
                    getRepos().getRelationalPackage().getCwmSqldataType())));
    }

    // implement FarragoSessionStmtValidator
    public FemJar findJarFromLiteralName(String jarName)
    {
        // TODO jvs 19-Jan-2005: support "thisjar" in deployment
        // descriptors
        SqlIdentifier qualifiedJarName;
        try {
            SqlParser sqlParser = new SqlParser(jarName);
            SqlNode sqlNode = sqlParser.parseExpression();
            qualifiedJarName = (SqlIdentifier) sqlNode;
        } catch (Throwable ex) {
            throw FarragoResource.instance().ValidatorRoutineInvalidJarName.ex(
                repos.getLocalizedObjectName(jarName));
        }
        return findSchemaObject(
            qualifiedJarName,
            FemJar.class);
    }

    private void checkValidated(CwmModelElement element)
    {
        if (element.getVisibility() == null) {
            throw new FarragoUnvalidatedDependencyException();
        }
    }

    // implement FarragoSessionStmtValidator
    public <T extends CwmModelElement> FarragoSessionResolvedObject<T>
    resolveSchemaObjectName(
        String [] names,
        Class<T> clazz)
    {
        FarragoSessionResolvedObject<T> resolved =
            new FarragoSessionResolvedObject<T>();
        if (names.length > 3) {
            // Max is catalog.schema.obj
            return null;
        } else if (names.length == 3) {
            resolved.catalogName = names[0];
            resolved.schemaName = names[1];
            resolved.objectName = names[2];
        } else if (names.length == 2) {
            resolved.catalogName = sessionVariables.catalogName;
            resolved.schemaName = names[0];
            resolved.objectName = names[1];
        } else if (names.length == 1) {
            if (sessionVariables.schemaName == null) {
                // TODO:  use names for context
                throw FarragoResource.instance().ValidatorNoDefaultSchema.ex();
            }
            resolved.catalogName = sessionVariables.catalogName;
            resolved.schemaName = sessionVariables.schemaName;
            resolved.objectName = names[0];
        } else {
            throw new IllegalArgumentException();
        }

        resolved.catalog = repos.getCatalog(resolved.catalogName);
        if (resolved.catalog == null) {
            // TODO:  throw ValidatorUnknownObject for catalog
            return null;
        }

        if (resolved.catalog instanceof FemDataServer) {
            // we don't have any metadata for direct references to
            // remote objects
            return resolved;
        }

        resolved.schema =
            FarragoCatalogUtil.getSchemaByName(
                resolved.catalog,
                resolved.schemaName);
        if (resolved.schema == null) {
            // TODO:  throw ValidatorUnknownObject for schema
            return null;
        }

        resolved.object =
            FarragoCatalogUtil.getModelElementByNameAndType(
                (Collection<CwmModelElement>) resolved.schema.getOwnedElement(),
                resolved.objectName,
                clazz);
        if (resolved.object == null) {
            return null;
        }

        return resolved;
    }

    public CwmNamedColumnSet getSampleDataset(
        CwmNamedColumnSet columnSet,
        String datasetName)
    {
        if (columnSet instanceof FemAbstractColumnSet) {
            for (
                FemSampleDataset dataset
                : (Collection<FemSampleDataset>)
                ((FemAbstractColumnSet) columnSet).getSampleDataset())
            {
                if (dataset.getName().equals(datasetName)) {
                    return (CwmNamedColumnSet) dataset.getUsedColumnSet();
                }
            }
        }

        // no sample found
        return null;
    }

    // implement FarragoSessionStmtValidator
    public void setParserPosition(SqlParserPos pos)
    {
        this.parserPos = pos;
    }

    private EigenbaseException newPositionalError(
        SqlValidatorException ex)
    {
        if (parserPos == null) {
            return parser.newPositionalError(ex);
        } else {
            String msg = parserPos.toString();
            return FarragoResource.instance().ValidatorPositionContext.ex(
                msg,
                ex);
        }
    }

    // implement FarragoSessionStmtValidator
    public void validateFeature(
        ResourceDefinition feature,
        SqlParserPos context)
    {
        FarragoSessionPersonality personality = getSession().getPersonality();
        if (personality.supportsFeature(feature)) {
            return;
        }
        EigenbaseException ex =
            new EigenbaseException(
                feature.instantiate(
                    EigenbaseResource.instance(),
                    Util.emptyObjectArray).toString(),
                null);
        if (context != null) {
            throw SqlUtil.newContextException(
                context,
                ex);
        } else {
            throw ex;
        }
    }

    // implement FarragoSessionStmtValidator
    public void setTimingTracer(
        EigenbaseTimingTracer timingTracer)
    {
        this.timingTracer = timingTracer;
    }

    // implement FarragoSessionStmtValidator
    public EigenbaseTimingTracer getTimingTracer()
    {
        return timingTracer;
    }

    // implement FarragoSessionStmtValidator
    public void setReposTxnContext(FarragoReposTxnContext reposTxnContext)
    {
        this.reposTxnContext = reposTxnContext;
    }

    public FarragoReposTxnContext getReposTxnContext()
    {
        return reposTxnContext;
    }

    // implement FarragoSessionStmtValidator
    public void validateDataType(SqlDataTypeSpec dataType)
        throws SqlValidatorException
    {
        // Check that every type is supported. For example, we don't support
        // columns of type LONG VARCHAR right now.

        final FarragoSessionPersonality personality =
            getSession().getPersonality();
        final String typeNameName = dataType.getTypeName().toString();
        final FarragoResource res = FarragoResource.instance();

        SqlTypeName typeName = SqlTypeName.get(typeNameName);
        if (typeName != null) {
            if (!personality.isSupportedType(typeName)) {
                throw EigenbaseResource.instance().TypeNotSupported.ex(
                    typeName.toString());
            }
        }

        CwmSqldataType type = findSqldataType(dataType.getTypeName());

        // Negative precision or scale in SqlDataTypeSpec indicates the
        // precision or scale was not specified
        Integer precision =
            (dataType.getPrecision() >= 0)
            ? Integer.valueOf(dataType.getPrecision())
            : null;
        Integer scale =
            (dataType.getScale() >= 0) ? Integer.valueOf(dataType.getScale())
            : null;

        // first, validate presence of modifiers
        if ((typeName != null) && typeName.allowsPrec()) {
            if (precision == null) {
                int p = typeName.getDefaultPrecision();
                if (p != -1) {
                    precision = p;
                }
            }
            if ((precision == null) && !typeName.allowsNoPrecNoScale()) {
                throw res.ValidatorPrecRequired.ex(
                    repos.getLocalizedObjectName(type));
            }
        } else {
            if (precision != null) {
                throw res.ValidatorPrecUnexpected.ex(
                    repos.getLocalizedObjectName(type));
            }
        }
        if ((typeName != null) && typeName.allowsScale()) {
            // assume scale is always optional
        } else {
            if (scale != null) {
                throw res.ValidatorScaleUnexpected.ex(
                    repos.getLocalizedObjectName(type));
            }
        }
        SqlTypeFamily typeFamily = null;
        if (typeName != null) {
            typeFamily = SqlTypeFamily.getFamilyForSqlType(typeName);
        }
        if (typeFamily == SqlTypeFamily.CHARACTER) {
            String charsetName = dataType.getCharSetName();
            if (JmiUtil.isBlank(charsetName)) {
                charsetName = repos.getDefaultCharsetName();
            } else {
                if (!Charset.isSupported(charsetName)) {
                    throw res.ValidatorCharsetUnsupported.ex(
                        dataType.getCharSetName());
                }
            }
            Charset charSet = Charset.forName(charsetName);
            if (charSet.newEncoder().maxBytesPerChar() > 1) {
                // TODO:  implement multi-byte character sets
                throw Util.needToImplement(charSet);
            }
        } else {
            if (!JmiUtil.isBlank(dataType.getCharSetName())) {
                throw res.ValidatorCharsetUnexpected.ex(
                    repos.getLocalizedObjectName(type));
            }
        }

        // now, enforce type-defined limits
        if (type instanceof CwmSqlsimpleType) {
            CwmSqlsimpleType simpleType = (CwmSqlsimpleType) type;

            if (precision != null) {
                if ((typeFamily == SqlTypeFamily.BINARY)
                    || (typeFamily == SqlTypeFamily.CHARACTER))
                {
                    Integer maximum = simpleType.getCharacterMaximumLength();
                    assert (maximum != null);
                    if (precision.intValue() > maximum.intValue()) {
                        throw res.ValidatorLengthExceeded.ex(
                            precision,
                            maximum);
                    }
                } else {
                    Integer maximum = simpleType.getNumericPrecision();
                    if (maximum == null) {
                        maximum = simpleType.getDateTimePrecision();
                    }
                    assert (maximum != null);
                    if (precision.intValue() > maximum.intValue()) {
                        throw res.ValidatorPrecisionExceeded.ex(
                            precision,
                            maximum);
                    }
                    if (typeFamily == SqlTypeFamily.NUMERIC) {
                        if (precision.intValue() <= 0) {
                            throw res.ValidatorPrecisionMustBePositive.ex();
                        }
                    }
                }
            }
            if (scale != null) {
                Integer maximum = simpleType.getNumericScale();
                assert (maximum != null);
                if (scale.intValue() > maximum.intValue()) {
                    throw res.ValidatorScaleExceeded.ex(
                        scale,
                        maximum);
                }
            }
        } else if (type instanceof FemSqlcollectionType) {
            FemSqlcollectionType collectionType = (FemSqlcollectionType) type;
            FemSqltypeAttribute componentType =
                (FemSqltypeAttribute) collectionType.getFeature().get(0);
            // TODO: Validate
        } else if (type instanceof FemUserDefinedType) {
            // nothing special to do for UDT's, which were already validated on
            // creation
        } else if (type instanceof FemSqlrowType) {
            FemSqlrowType rowType = (FemSqlrowType) type;
            for (
                FemAbstractAttribute column
                : Util.cast(rowType.getFeature(), FemAbstractAttribute.class))
            {
                // TODO: Validate
            }
        } else {
            throw Util.needToImplement(type);
        }
    }
}

// End FarragoStmtValidator.java

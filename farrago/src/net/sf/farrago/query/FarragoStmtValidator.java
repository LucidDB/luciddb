/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later Eigenbase-approved version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307  USA
*/
package net.sf.farrago.query;

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

import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.util.Util;


/**
 * FarragoStmtValidator is a default implementation of the
 * {@link FarragoSessionStmtValidator} interface.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoStmtValidator extends FarragoCompoundAllocation
    implements FarragoSessionStmtValidator
{
    //~ Instance fields -------------------------------------------------------

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
    
    private SqlParserPos parserPos;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FarragoStmtValidator object.
     *
     * @param repos repos to use for object definitions
     *
     * @param fennelDbHandle handle to Fennel database to access
     *
     * @param session invoking session
     *
     * @param codeCache FarragoObjectCache to use for caching code snippets
     * needed during preparation
     *
     * @param sharedDataWrapperCache FarragoObjectCache to use for caching
     * FarragoMedDataWrapper instances
     *
     * @param indexMap FarragoSessionIndexMap to use for index access
     */
    public FarragoStmtValidator(
        FarragoRepos repos,
        FennelDbHandle fennelDbHandle,
        FarragoSession session,
        FarragoObjectCache codeCache,
        FarragoObjectCache sharedDataWrapperCache,
        FarragoSessionIndexMap indexMap)
    {
        this.repos = repos;
        this.fennelDbHandle = fennelDbHandle;
        this.codeCache = codeCache;
        this.indexMap = indexMap;
        this.session = session;
        this.sharedDataWrapperCache = sharedDataWrapperCache;

        parser = session.newParser();
        // clone session variables so that any context changes we make during
        // validation are transient
        sessionVariables = session.getSessionVariables().cloneVariables();
        typeFactory = new FarragoTypeFactoryImpl(repos);
        dataWrapperCache =
            new FarragoDataWrapperCache(this, sharedDataWrapperCache, repos,
                fennelDbHandle);
    }

    //~ Methods ---------------------------------------------------------------

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
                FarragoResource.instance().newValidatorUnknownObjectInScope(
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
    public String[] getAllSchemaObjectNames(String[] names)
    {
        // use default catalog.  If not set, don't do anything
        CwmCatalog catalog = repos.getCatalog(sessionVariables.catalogName);
        if (catalog == null) {
            return Util.emptyStringArray;
        }

        // look for both schema and object names
        if (names.length == 1) {
            // get all schema names 
            List schemaNames = getAllObjectNamesByType(
                catalog.getOwnedElement(),
                FemLocalSchema.class);

            // if default schema is set, get all table names in this schema
            FemLocalSchema schema = FarragoCatalogUtil.getSchemaByName(
                repos, catalog, sessionVariables.schemaName);
            if (schema != null) {
                List tableNames = getAllObjectNamesByType(
                    schema.getOwnedElement(),
                    FemLocalTable.class);
                schemaNames.addAll(tableNames);
            }
            return (String[]) schemaNames.toArray(Util.emptyStringArray);
        }
        // looking for table names under the specified schema
        else if (names.length == 2) {
            FemLocalSchema schema = FarragoCatalogUtil.getSchemaByName(
                repos, catalog, names[0]);
            if (schema != null) {
                List tableNames = getAllObjectNamesByType(
                    schema.getOwnedElement(),
                    FemLocalTable.class);
                return (String []) tableNames.toArray(Util.emptyStringArray);
            }
            else {
                return Util.emptyStringArray;
            }
        }
        // currently not supporting the likes of SALES.EMPS.$DUMMY
        else {
            return Util.emptyStringArray;
        }
    }

    // get all the object names of 'type' in the 'collection'
    private List getAllObjectNamesByType(Collection collection, Class type)
    {
        Iterator iter = collection.iterator();
        List list = new ArrayList();
        while (iter.hasNext()) {
            CwmModelElement element = (CwmModelElement) iter.next();
            if (type.isInstance(element)) {
                list.add(element.getName());
            }
        }
        return list;
    }

    private CwmCatalog lookupCatalog(
        String catalogName, boolean throwIfNotFound)
    {
        CwmCatalog catalog = getRepos().getCatalog(catalogName);

        if ((catalog == null) && throwIfNotFound) {
            throw newPositionalError(
                FarragoResource.instance().newValidatorUnknownObject(
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
            catalog = lookupCatalog(
                getSessionVariables().catalogName, throwIfNotFound);
            simpleName = schemaName.getSimple();
        }
        if (catalog == null) {
            return null;
        }
        FemLocalSchema schema =
            FarragoCatalogUtil.getSchemaByName(repos, catalog, simpleName);
        // REVIEW:  parser context may be past schema name already
        if (schema == null) {
            if (!throwIfNotFound) {
                return null;
            }
            throw newPositionalError(
                FarragoResource.instance().newValidatorUnknownObject(
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
            (FemDataWrapper) FarragoCatalogUtil.getModelElementByName(
                getRepos().getMedPackage().getFemDataWrapper().refAllOfType(),
                wrapperName.getSimple());
        if (wrapper != null) {
            if (wrapper.isForeign() != isForeign) {
                wrapper = null;
            }
        }
        if (wrapper == null) {
            throw newPositionalError(
                FarragoResource.instance().newValidatorUnknownObject(
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
        FemDataServer server =
            (FemDataServer) FarragoCatalogUtil.getModelElementByName(
                getRepos().getMedPackage().getFemDataServer().refAllOfType(),
                serverName.getSimple());
        if (server == null) {
            throw newPositionalError(
                FarragoResource.instance().newValidatorUnknownObject(
                    getRepos().getLocalizedObjectName(
                        null,
                        serverName.getSimple(),
                        getRepos().getMedPackage().getFemDataServer())));
        }
        return server;
    }

    // implement FarragoSessionStmtValidator
    public FemDataServer getDefaultLocalDataServer()
    {
        String dataServerName = session.getDefaultLocalDataServerName();
        return findDataServer(new SqlIdentifier(dataServerName, null));
    }

    // implement FarragoSessionStmtValidator
    public CwmModelElement findSchemaObject(
        SqlIdentifier qualifiedName,
        RefClass refClass)
    {
        FarragoSessionResolvedObject resolved =
            resolveSchemaObjectName(qualifiedName.names, refClass);

        CwmModelElement element = null;
        String schemaName = null;

        if (resolved != null) {
            schemaName = resolved.schemaName;
            if (resolved.object != null) {
                element = resolved.object;
            }
        }

        if (element == null) {
            throw newPositionalError(
                FarragoResource.instance().newValidatorUnknownObject(
                getRepos().getLocalizedObjectName(schemaName,
                    qualifiedName.names[qualifiedName.names.length - 1],
                    refClass)));
        }

        return element;
    }

    // implement FarragoSessionStmtValidator
    public List findRoutineOverloads(
        SqlIdentifier invocationName,
        ProcedureType routineType)
    {
        FarragoSessionVariables sessionVariables = getSessionVariables();
        Collection routines;
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
            SqlIdentifier schemaId = new SqlIdentifier(schemaNames, null);
            FemLocalSchema schema = findSchema(schemaId);
            routines = schema.getOwnedElement();
        } else {
            simpleName = invocationName.getSimple();
            // filter to only schemas on SQL-path
            routines = new ArrayList();
            Iterator iter = sessionVariables.schemaSearchPath.iterator();
            while (iter.hasNext()) {
                SqlIdentifier id = (SqlIdentifier) iter.next();
                CwmSchema searchedSchema = lookupSchema(id, false);
                if (searchedSchema == null) {
                    continue;
                }
                FarragoCatalogUtil.filterTypedModelElements(
                    searchedSchema.getOwnedElement(),
                    routines,
                    FemRoutine.class);
            }
        }
        List overloads = new ArrayList();
        Iterator iter = routines.iterator();
        while (iter.hasNext()) {
            Object obj = iter.next();
            if (!(obj instanceof FemRoutine)) {
                continue;
            }
            FemRoutine routine = (FemRoutine) obj;
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
        Collection types;
        CwmModelElement modelElement;

        if (!typeName.isSimple()) {
            modelElement = findSchemaObject(
                typeName,
                getRepos().getSql2003Package().getFemUserDefinedType());
            checkValidated(modelElement);
            return (CwmSqldataType) modelElement;
        }

        String simpleName = typeName.getSimple();

        types =
            getRepos().getRelationalPackage().getCwmSqlsimpleType().refAllOfClass();
        modelElement =
            FarragoCatalogUtil.getModelElementByName(types, simpleName);
        if (modelElement != null) {
            return (CwmSqldataType) modelElement;
        }

        types =
            getRepos().getDataTypesPackage().getCwmTypeAlias().refAllOfClass();
        modelElement = FarragoCatalogUtil.getModelElementByName(
            types, simpleName);
        if (modelElement != null) {
            CwmTypeAlias alias = (CwmTypeAlias) modelElement;
            return (CwmSqldataType) alias.getType();
        }

        Iterator iter = sessionVariables.schemaSearchPath.iterator();
        while (iter.hasNext()) {
            SqlIdentifier id = (SqlIdentifier) iter.next();
            CwmSchema searchedSchema = lookupSchema(id, false);
            if (searchedSchema == null) {
                continue;
            }
            modelElement = FarragoCatalogUtil.getModelElementByNameAndType(
                searchedSchema.getOwnedElement(),
                simpleName,
                repos.getSql2003Package().getFemUserDefinedType());
            if (modelElement != null) {
                checkValidated(modelElement);
                return (CwmSqldataType) modelElement;
            }
        }
            
        throw newPositionalError(
            FarragoResource.instance().newValidatorUnknownObject(
                getRepos().getLocalizedObjectName(
                    null,
                    simpleName,
                    getRepos().getRelationalPackage().getCwmSqldataType())));
    }

    private void checkValidated(CwmModelElement element)
    {
        if (element.getVisibility() == null) {
            throw new FarragoUnvalidatedDependencyException();
        }
    }

    // implement FarragoSessionStmtValidator
    public FarragoSessionResolvedObject resolveSchemaObjectName(
        String [] names,
        RefClass refClass)
    {
        FarragoSessionResolvedObject resolved =
            new FarragoSessionResolvedObject();
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
                throw FarragoResource.instance().newValidatorNoDefaultSchema();
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
                repos, resolved.catalog, resolved.schemaName);
        if (resolved.schema == null) {
            // TODO:  throw ValidatorUnknownObject for schema
            return null;
        }

        resolved.object =
            FarragoCatalogUtil.getModelElementByNameAndType(
                resolved.schema.getOwnedElement(),
                resolved.objectName,
                refClass);
        if (resolved.object == null) {
            return null;
        }

        return resolved;
    }

    // implement FarragoSessionStmtValidator
    public void setParserPosition(SqlParserPos pos)
    {
        this.parserPos = pos;
    }
    
    private FarragoException newPositionalError(
        SqlValidatorException ex)
    {
        if (parserPos == null) {
            return parser.newPositionalError(ex);
        } else {
            String msg = parserPos.toString();
            return FarragoResource.instance().newValidatorPositionContext(
                msg, ex);
        }
    }
}


// End FarragoStmtValidator.java

/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
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
        sessionVariables = session.getSessionVariables();
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
            (CwmColumn) getRepos().getModelElement(
                namedColumnSet.getFeature(),
                columnName);
        if (column == null) {
            throw FarragoResource.instance().newValidatorUnknownColumn(
                columnName,
                namedColumnSet.getName(),
                parser.getCurrentPosition().toString());
        }
        return column;
    }

    // implement FarragoSessionStmtValidator
    public CwmCatalog findCatalog(String catalogName)
    {
        CwmCatalog catalog = getRepos().getCwmCatalog(catalogName);

        if (catalog == null) {
            throw FarragoResource.instance().newValidatorUnknownObject(
                getRepos().getLocalizedObjectName(
                    null,
                    catalogName,
                    getRepos().relationalPackage.getCwmCatalog()),
                parser.getCurrentPosition().toString());
        }
        return catalog;
    }

    // implement FarragoSessionStmtValidator
    public CwmCatalog getDefaultCatalog()
    {
        return findCatalog(getSessionVariables().catalogName);
    }

    // implement FarragoSessionStmtValidator
    public CwmSchema findSchema(SqlIdentifier schemaName)
    {
        CwmCatalog catalog;
        String simpleName;
        if (schemaName.names.length == 2) {
            catalog = findCatalog(schemaName.names[0]);
            simpleName = schemaName.names[1];
        } else {
            catalog = getDefaultCatalog();
            simpleName = schemaName.getSimple();
        }
        CwmSchema schema = getRepos().getSchema(catalog, simpleName);

        // REVIEW:  parser context may be past schema name already
        if (schema == null) {
            throw FarragoResource.instance().newValidatorUnknownObject(
                getRepos().getLocalizedObjectName(
                    catalog.getName(),
                    simpleName,
                    getRepos().relationalPackage.getCwmSchema()),
                parser.getCurrentPosition().toString());
        }
        return schema;
    }

    // implement FarragoSessionStmtValidator
    public FemDataWrapper findDataWrapper(
        SqlIdentifier wrapperName,
        boolean isForeign)
    {
        FemDataWrapper wrapper =
            (FemDataWrapper) getRepos().getModelElement(
                getRepos().medPackage.getFemDataWrapper().refAllOfType(),
                wrapperName.getSimple());
        if (wrapper != null) {
            if (wrapper.isForeign() != isForeign) {
                wrapper = null;
            }
        }
        if (wrapper == null) {
            throw FarragoResource.instance().newValidatorUnknownObject(
                getRepos().getLocalizedObjectName(
                    null,
                    wrapperName.getSimple(),
                    getRepos().medPackage.getFemDataWrapper()),
                parser.getCurrentPosition().toString());
        }
        return wrapper;
    }

    // implement FarragoSessionStmtValidator
    public FemDataServer findDataServer(SqlIdentifier serverName)
    {
        FemDataServer server =
            (FemDataServer) getRepos().getModelElement(
                getRepos().medPackage.getFemDataServer().refAllOfType(),
                serverName.getSimple());
        if (server == null) {
            throw FarragoResource.instance().newValidatorUnknownObject(
                getRepos().getLocalizedObjectName(
                    null,
                    serverName.getSimple(),
                    getRepos().medPackage.getFemDataServer()),
                parser.getCurrentPosition().toString());
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
        CwmSchema schema,
        SqlIdentifier qualifiedName,
        RefClass refClass)
    {
        FarragoSessionVariables sessionVariables = getSessionVariables();
        if (schema != null) {
            sessionVariables = sessionVariables.cloneVariables();
            sessionVariables.schemaCatalogName =
                schema.getNamespace().getName();
            sessionVariables.schemaName = schema.getName();
        }

        FarragoSessionResolvedObject resolved =
            resolveSchemaObjectName(qualifiedName.names);

        CwmModelElement element = null;

        String schemaName = null;

        if (resolved != null) {
            schemaName = resolved.schemaName;
            if (resolved.object != null) {
                element = resolved.object;
                if (!element.refIsInstanceOf(
                            refClass.refMetaObject(),
                            true)) {
                    element = null;
                }
            }
        }

        if (element == null) {
            if ((schemaName == null) && (schema != null)) {
                schemaName = schema.getName();
            }
            throw FarragoResource.instance().newValidatorUnknownObject(
                getRepos().getLocalizedObjectName(schemaName,
                    qualifiedName.names[qualifiedName.names.length - 1],
                    refClass),
                parser.getCurrentPosition().toString());
        }

        return element;
    }

    // implement FarragoSessionStmtValidator
    public List findRoutineOverloads(
        CwmSchema schema,
        String invocationName,
        ProcedureType routineType)
    {
        FarragoSessionVariables sessionVariables = getSessionVariables();
        Collection routines;
        if (schema != null) {
            routines = schema.getOwnedElement();
        } else {
            // TODO jvs 1-Jan-2005: filter to only schemas on
            // SQL-path
            routines =
                getRepos().getSql2003Package().getFemRoutine().refAllOfType();
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
            if (!routine.getInvocationName().equals(invocationName)) {
                continue;
            }
            overloads.add(routine);
        }
        return overloads;
    }
    
    // implement FarragoSessionStmtValidator
    public CwmSqldataType findSqldataType(String typeName)
    {
        Collection types =
            getRepos().relationalPackage.getCwmSqlsimpleType().refAllOfClass();
        CwmModelElement modelElement =
            getRepos().getModelElement(types, typeName);
        if (modelElement != null) {
            return (CwmSqldataType) modelElement;
        }

        types = getRepos().datatypesPackage.getCwmTypeAlias().refAllOfClass();
        modelElement = getRepos().getModelElement(types, typeName);
        if (modelElement != null) {
            CwmTypeAlias alias = (CwmTypeAlias) modelElement;
            return (CwmSqldataType) alias.getType();
        }

        throw FarragoResource.instance().newValidatorUnknownObject(
            getRepos().getLocalizedObjectName(
                null,
                typeName,
                getRepos().relationalPackage.getCwmSqldataType()),
            parser.getCurrentPosition().toString());
    }

    // implement FarragoSessionStmtValidator
    public FarragoSessionResolvedObject resolveSchemaObjectName(
        String [] names)
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
            resolved.catalogName = sessionVariables.schemaCatalogName;
            resolved.schemaName = sessionVariables.schemaName;
            resolved.objectName = names[0];
        } else {
            throw new IllegalArgumentException();
        }

        resolved.catalog = repos.getCwmCatalog(resolved.catalogName);
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
            repos.getSchema(resolved.catalog, resolved.schemaName);
        if (resolved.schema == null) {
            // TODO:  throw ValidatorUnknownObject for schema
            return null;
        }

        resolved.object =
            repos.getModelElement(
                resolved.schema.getOwnedElement(),
                resolved.objectName);
        if (resolved.object == null) {
            return null;
        }

        return resolved;
    }
}


// End FarragoStmtValidator.java

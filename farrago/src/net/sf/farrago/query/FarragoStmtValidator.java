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

import net.sf.farrago.resource.*;
import net.sf.farrago.session.*;
import net.sf.farrago.catalog.*;
import net.sf.farrago.fennel.*;
import net.sf.farrago.type.*;
import net.sf.farrago.util.*;
import net.sf.farrago.namespace.util.*;

import net.sf.farrago.cwm.core.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.cwm.datatypes.*;
import net.sf.farrago.fem.med.*;

import org.eigenbase.sql.*;

import java.util.*;

import javax.jmi.reflect.*;

/**
 * FarragoStmtValidator is a default implementation of the
 * {@link FarragoSessionStmtValidator} interface.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoStmtValidator
    extends FarragoCompoundAllocation
    implements FarragoSessionStmtValidator
{
    private final FarragoCatalog catalog;

    private final FennelDbHandle fennelDbHandle;

    private final FarragoSession session;

    private final FarragoTypeFactory typeFactory;

    private final FarragoConnectionDefaults connectionDefaults;

    private final FarragoObjectCache codeCache;

    private final FarragoDataWrapperCache dataWrapperCache;

    private final FarragoIndexMap indexMap;

    private final FarragoObjectCache sharedDataWrapperCache;
    
    private final FarragoSessionParser parser;

    /**
     * Creates a new FarragoStmtValidator object.
     *
     * @param catalog catalog to use for object definitions
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
     * @param indexMap FarragoIndexMap to use for index access
     */
    public FarragoStmtValidator(
        FarragoCatalog catalog,
        FennelDbHandle fennelDbHandle,
        FarragoSession session,
        FarragoObjectCache codeCache,
        FarragoObjectCache sharedDataWrapperCache,
        FarragoIndexMap indexMap)
    {
        this.catalog = catalog;
        this.fennelDbHandle = fennelDbHandle;
        this.codeCache = codeCache;
        this.indexMap = indexMap;
        this.session = session;
        this.sharedDataWrapperCache = sharedDataWrapperCache;
        
        parser = session.newParser();
        connectionDefaults = session.getConnectionDefaults();
        typeFactory = new FarragoTypeFactoryImpl(catalog);
        dataWrapperCache = new FarragoDataWrapperCache(
            this,sharedDataWrapperCache,catalog,fennelDbHandle);
    }

    // implement FarragoSessionStmtValidator
    public FarragoSessionParser getParser()
    {
        return parser;
    }

    // implement FarragoSessionStmtValidator
    public FarragoCatalog getCatalog()
    {
        return catalog;
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
    public FarragoConnectionDefaults getConnectionDefaults()
    {
        return connectionDefaults;
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
    public FarragoIndexMap getIndexMap()
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
            (CwmColumn) getCatalog().getModelElement(
                namedColumnSet.getFeature(),columnName);
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
        CwmCatalog cwmCatalog = getCatalog().getCwmCatalog(catalogName);

        if (cwmCatalog == null) {
            throw FarragoResource.instance().newValidatorUnknownObject(
                getCatalog().getLocalizedObjectName(
                    null,
                    catalogName,
                    getCatalog().relationalPackage.getCwmCatalog()),
                parser.getCurrentPosition().toString());
        }
        return cwmCatalog;
    }

    // implement FarragoSessionStmtValidator
    public CwmCatalog getDefaultCatalog()
    {
        return findCatalog(getConnectionDefaults().catalogName);
    }

    // implement FarragoSessionStmtValidator
    public CwmSchema findSchema(SqlIdentifier schemaName)
    {
        CwmCatalog cwmCatalog;
        String simpleName;
        if (schemaName.names.length == 2) {
            cwmCatalog = findCatalog(schemaName.names[0]);
            simpleName = schemaName.names[1];
        } else {
            cwmCatalog = getDefaultCatalog();
            simpleName = schemaName.getSimple();
        }
        CwmSchema schema = getCatalog().getSchema(cwmCatalog,simpleName);
        // REVIEW:  parser context may be past schema name already
        if (schema == null) {
            throw FarragoResource.instance().newValidatorUnknownObject(
                getCatalog().getLocalizedObjectName(
                    cwmCatalog.getName(),
                    simpleName,
                    getCatalog().relationalPackage.getCwmSchema()),
                parser.getCurrentPosition().toString());
        }
        return schema;
    }

    // implement FarragoSessionStmtValidator
    public FemDataWrapper findDataWrapper(
        SqlIdentifier wrapperName,boolean isForeign)
    {
        FemDataWrapper wrapper = (FemDataWrapper)
            getCatalog().getModelElement(
                getCatalog().medPackage.getFemDataWrapper().refAllOfType(),
                wrapperName.getSimple());
        if (wrapper != null) {
            if (wrapper.isForeign() != isForeign) {
                wrapper = null;
            }
        }
        if (wrapper == null) {
            throw FarragoResource.instance().newValidatorUnknownObject(
                getCatalog().getLocalizedObjectName(
                    null,
                    wrapperName.getSimple(),
                    getCatalog().medPackage.getFemDataWrapper()),
                parser.getCurrentPosition().toString());
        }
        return wrapper;
    }

    // implement FarragoSessionStmtValidator
    public FemDataServer findDataServer(SqlIdentifier serverName)
    {
        FemDataServer server = (FemDataServer)
            getCatalog().getModelElement(
                getCatalog().medPackage.getFemDataServer().refAllOfType(),
                serverName.getSimple());
        if (server == null) {
            throw FarragoResource.instance().newValidatorUnknownObject(
                getCatalog().getLocalizedObjectName(
                    null,
                    serverName.getSimple(),
                    getCatalog().medPackage.getFemDataServer()),
                parser.getCurrentPosition().toString());
        }
        return server;
    }

    // implement FarragoSessionStmtValidator
    public FemDataServer getDefaultLocalDataServer()
    {
        // TODO:  make this configurable
        return findDataServer(new SqlIdentifier("SYS_ROWSTORE",null));
    }

    // implement FarragoSessionStmtValidator
    public CwmModelElement findSchemaObject(
        CwmSchema schema,
        SqlIdentifier qualifiedName,
        RefClass refClass)
    {
        FarragoConnectionDefaults fcd = getConnectionDefaults();
        if (schema != null) {
            fcd = fcd.cloneDefaults();
            fcd.schemaCatalogName = schema.getNamespace().getName();
            fcd.schemaName = schema.getName();
        }

        FarragoCatalog.ResolvedSchemaObject resolved =
            getCatalog().resolveSchemaObjectName(
                fcd,
                qualifiedName.names);

        CwmModelElement element = null;

        String schemaName = null;

        if (resolved != null) {
            schemaName = resolved.schemaName;
            if (resolved.object != null) {
                element = resolved.object;
                if (!element.refIsInstanceOf(refClass.refMetaObject(),true)) {
                    element = null;
                }
            }
        }

        if (element == null) {
            if ((schemaName == null) && (schema != null)) {
                schemaName = schema.getName();
            }
            throw FarragoResource.instance().newValidatorUnknownObject(
                getCatalog().getLocalizedObjectName(
                    schemaName,
                    qualifiedName.names[qualifiedName.names.length - 1],
                    refClass),
                parser.getCurrentPosition().toString());
        }

        return element;
    }

    // implement FarragoSessionStmtValidator
    public CwmSqldataType findSqldataType(String typeName)
    {
        Collection types =
            getCatalog().relationalPackage.
            getCwmSqlsimpleType().refAllOfClass();
        CwmModelElement modelElement =
            getCatalog().getModelElement(types,typeName);
        if (modelElement != null) {
            return (CwmSqldataType) modelElement;
        }

        types = getCatalog().datatypesPackage.getCwmTypeAlias().refAllOfClass();
        modelElement = getCatalog().getModelElement(types,typeName);
        if (modelElement != null) {
            CwmTypeAlias alias = (CwmTypeAlias) modelElement;
            return (CwmSqldataType) alias.getType();
        }

        throw FarragoResource.instance().newValidatorUnknownObject(
            getCatalog().getLocalizedObjectName(
                null,
                typeName,
                getCatalog().relationalPackage.getCwmSqldataType()),
            parser.getCurrentPosition().toString());
    }
}

// End FarragoStmtValidator.java

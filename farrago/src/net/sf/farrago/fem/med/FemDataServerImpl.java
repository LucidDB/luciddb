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

package net.sf.farrago.fem.med;

import net.sf.farrago.catalog.*;
import net.sf.farrago.ddl.*;
import net.sf.farrago.type.*;
import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.core.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.util.*;

import net.sf.saffron.util.*;
import net.sf.saffron.core.*;

import org.netbeans.mdr.handlers.*;
import org.netbeans.mdr.storagemodel.*;

import java.util.*;

/**
 * FemDataServerImpl is a custom implementation for FemDataServer.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class FemDataServerImpl extends InstanceHandler
    implements FemDataServer, DdlValidatedElement, DdlStoredElement
{
    /**
     * Creates a new FemDataServerImpl object.
     *
     * @param storable .
     */
    protected FemDataServerImpl(StorableObject storable)
    {
        super(storable);
    }

    // implement DdlValidatedElement
    public void validateDefinition(DdlValidator validator,boolean creation)
    {
        FarragoCatalog catalog = validator.getCatalog();

        // since servers are in the same namespace with CWM catalogs,
        // need a special name uniquness check here
        validator.validateUniqueNames(
            catalog.getCwmCatalog(FarragoCatalog.SYSBOOT_CATALOG_NAME),
            catalog.relationalPackage.getCwmCatalog().refAllOfType(),
            false);

        try {
            // validate that we can successfully initialize the server
            loadFromCache(validator.getDataWrapperCache());
        } catch (Throwable ex) {
            throw validator.res.newValidatorDataServerInvalid(
                catalog.getLocalizedObjectName(this,null),
                ex);
        }

        // REVIEW jvs 18-April-2004:  This uses default charset/collation
        // info from local catalog, but should really allow foreign
        // servers to override.
        catalog.initializeCwmCatalog(this);

        // REVIEW jvs 18-April-2004:  Query the plugin for these?
        if (getType() == null) {
            setType("UNKNOWN");
        }
        if (getVersion() == null) {
            setVersion("UNKNOWN");
        }

        validator.createDependency(
            this,
            Collections.singleton(getWrapper()),
            "WrapperAccessesServer");
    }

    // implement DdlValidatedElement
    public void validateDeletion(DdlValidator validator,boolean truncation)
    {
    }
    
    /**
     * Loads and caches an accessor for this server, or uses a cached
     * instance.
     *
     * @param cache .
     *
     * @return loaded server accessor
     */
    public FarragoMedDataServer loadFromCache(
        FarragoDataWrapperCache cache)
    {
        Properties props =
            FemDataWrapperImpl.getStorageOptionsAsProperties(this);
        {
            String val;
            if ((val = getType()) != null)
                props.setProperty(FarragoMedDataServer.PROP_SERVER_TYPE, val);
            if ((val = getVersion()) != null)
                props.setProperty(FarragoMedDataServer.PROP_SERVER_VERSION, val);
        }

        FemDataWrapperImpl femDataWrapper =
            (FemDataWrapperImpl) getWrapper();

        FarragoMedDataWrapper dataWrapper =
            femDataWrapper.loadFromCache(cache);

        return cache.loadServer(
            refMofId(),
            dataWrapper,
            props);
    }

    /**
     * Creates a FarragoMedColumnSet representation for a BaseColumnSet
     * provided by this server.
     *
     * @param cache cache for loading server
     *
     * @param catalog catalog for object names
     *
     * @param typeFactory factory for data types
     *
     * @param baseColumnSet column set definition
     *
     * @return loaded column set
     */
    public FarragoMedColumnSet loadColumnSetFromCache(
        FarragoDataWrapperCache cache,
        FarragoCatalog catalog,
        FarragoTypeFactory typeFactory,
        FemBaseColumnSet baseColumnSet)
    {
        String [] qualifiedName = new String [] 
            {
                baseColumnSet.getNamespace().getNamespace().getName(),
                baseColumnSet.getNamespace().getName(),
                baseColumnSet.getName()
            };

        Properties props =
            FemDataWrapperImpl.getStorageOptionsAsProperties(baseColumnSet);

        Map columnPropMap = new HashMap();

        SaffronType rowType = typeFactory.createColumnSetType(baseColumnSet);
            
        Iterator iter = baseColumnSet.getFeature().iterator();
        while (iter.hasNext()) {
            Object obj = iter.next();
            FemStoredColumn column = (FemStoredColumn) obj;
            columnPropMap.put(
                column.getName(),
                FemDataWrapperImpl.getStorageOptionsAsProperties(column));
        }
        
        FarragoMedDataServer medServer = loadFromCache(cache);

        FarragoMedColumnSet loadedColumnSet;
        try {
            loadedColumnSet = medServer.newColumnSet(
                qualifiedName,
                props,
                typeFactory,
                rowType,
                columnPropMap);
        } catch (Throwable ex) {
            throw FarragoResource.instance().newForeignTableAccessFailed(
                catalog.getLocalizedObjectName(baseColumnSet,null),
                ex);
        }

        if (rowType != null) {
            assert(rowType.equals(loadedColumnSet.getRowType()));
        }
        
        return loadedColumnSet;
    }

    FarragoMedColumnSet validateColumnSet(
        DdlValidator validator,FemBaseColumnSet baseColumnSet)
    {
        FarragoMedColumnSet columnSet;

        try {
            // validate that we can successfully initialize the table
            columnSet = loadColumnSetFromCache(
                validator.getDataWrapperCache(),
                validator.getCatalog(),
                validator.getTypeFactory(),
                baseColumnSet);
        } catch (Throwable ex) {
            throw validator.res.newValidatorDataServerTableInvalid(
                validator.getCatalog().getLocalizedObjectName(
                    baseColumnSet,baseColumnSet.refClass()),
                ex);
        }
        
        validator.createDependency(
            baseColumnSet,
            Collections.singleton(this),
            "ServerProvidesColumnSet");

        return columnSet;
    }

    // implement DdlStoredElement
    public void createStorage(DdlValidator validator)
    {
    }

    // implement DdlStoredElement
    public void deleteStorage(DdlValidator validator)
    {
        validator.discardDataWrapper(this);
    }

    // implement DdlStoredElement
    public void truncateStorage(DdlValidator validator)
    {
    }
}

// End FemDataServerImpl.java

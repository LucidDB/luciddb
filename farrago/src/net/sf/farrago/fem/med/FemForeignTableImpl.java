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
import net.sf.farrago.cwm.core.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.util.*;
import net.sf.farrago.type.*;

import net.sf.saffron.core.*;
import net.sf.saffron.util.*;

import org.netbeans.mdr.handlers.*;
import org.netbeans.mdr.storagemodel.*;

import java.util.*;

/**
 * FemForeignTableImpl is a custom implementation for FemForeignTable.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class FemForeignTableImpl extends InstanceHandler
    implements FemForeignTable, DdlValidatedElement
{
    /**
     * Creates a new FemForeignTableImpl object.
     *
     * @param storable .
     */
    protected FemForeignTableImpl(StorableObject storable)
    {
        super(storable);
    }

    // implement DdlValidatedElement
    public void validateDefinition(DdlValidator validator,boolean creation)
    {
        FarragoCatalog catalog = validator.getCatalog();
        FarragoTypeFactory typeFactory = validator.getTypeFactory();

        validator.validateUniqueNames(this,getFeature(),false);

        if (!getFeature().isEmpty()) {
            // columns were specified; we are to validate them
            Iterator iter = getFeature().iterator();
            while (iter.hasNext()) {
                FemForeignColumn column = (FemForeignColumn) iter.next();
                CwmColumnImpl.validateType(validator,column);
            }
        }
            
        FarragoMedColumnSet columnSet;
        try {
            // validate that we can successfully initialize the table
            columnSet = loadFromCache(
                validator.getDataWrapperCache(),
                catalog,
                typeFactory);
        } catch (Throwable ex) {
            throw validator.res.newValidatorForeignTableInvalid(
                catalog.getLocalizedObjectName(this,null),
                ex);
        }

        List columnList = getFeature();
        if (columnList.isEmpty()) {
            // derive column information
            SaffronType rowType = columnSet.getRowType();
            int n = rowType.getFieldCount();
            SaffronField [] fields = rowType.getFields();
            for (int i = 0; i < n; ++i) {
                CwmColumn column = catalog.newFemForeignColumn();
                columnList.add(column);
                typeFactory.convertFieldToCwmColumn(fields[i],column);
                CwmColumnImpl.validateType(validator,column);
            }
        }

        validator.createDependency(
            this,
            Collections.singleton(getDataServer(catalog)),
            "ServerStoresTable");
    }

    // implement DdlValidatedElement
    public void validateDeletion(DdlValidator validator,boolean truncation)
    {
    }
    
    /**
     * Finds the definition for the data server storing this foreign table.
     *
     * @param catalog catalog storing association
     *
     * @return server definition
     */
    public FemDataServerImpl getDataServer(FarragoCatalog catalog)
    {
        return (FemDataServerImpl)
            catalog.medPackage.getServerStoresTable().getServer(this);
    }

    /**
     * Creates a FarragoMedColumnSet representation for this foreign table.
     *
     * @param cache cache for loading server
     *
     * @return loaded table
     */
    public FarragoMedColumnSet loadFromCache(
        FarragoDataWrapperCache cache,
        FarragoCatalog catalog,
        FarragoTypeFactory typeFactory)
    {
        String [] qualifiedName = new String [] 
            {
                getNamespace().getNamespace().getName(),
                getNamespace().getName(),
                getName()
            };

        Properties props =
            FemDataWrapperImpl.getStorageOptionsAsProperties(this);

        Map columnPropMap = new HashMap();

        SaffronType rowType = typeFactory.createColumnSetType(this);
            
        Iterator iter = getFeature().iterator();
        while (iter.hasNext()) {
            FemForeignColumn column = (FemForeignColumn) iter.next();
            columnPropMap.put(
                column.getName(),
                FemDataWrapperImpl.getStorageOptionsAsProperties(column));
        }
        
        FemDataServerImpl femServer = getDataServer(catalog);

        FarragoMedDataServer server = femServer.loadFromCache(cache);

        FarragoMedColumnSet columnSet;
        try {
            columnSet = server.newColumnSet(
                qualifiedName,
                props,
                typeFactory,
                rowType,
                columnPropMap);
        } catch (Throwable ex) {
            throw FarragoResource.instance().newForeignTableAccessFailed(
                catalog.getLocalizedObjectName(this,null),
                ex);
        }

        if (rowType != null) {
            assert(rowType.equals(columnSet.getRowType()));
        }
        
        return columnSet;
    }
}

// End FemForeignTableImpl.java

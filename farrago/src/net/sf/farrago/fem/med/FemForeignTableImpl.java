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
        if (!creation) {
            return;
        }
        
        FarragoCatalog catalog = validator.getCatalog();
        FarragoTypeFactory typeFactory = validator.getTypeFactory();

        FemDataServerImpl dataServer = (FemDataServerImpl) getServer();
        FemDataWrapper dataWrapper = dataServer.getWrapper();
        if (!dataWrapper.isForeign()) {
            throw validator.res.newValidatorForeignTableButLocalWrapper(
                catalog.getLocalizedObjectName(this,null),
                catalog.getLocalizedObjectName(dataWrapper,null));
        }

        validator.validateUniqueNames(this,getFeature(),false);

        if (!getFeature().isEmpty()) {
            // columns were specified; we are to validate them
            Iterator iter = getFeature().iterator();
            while (iter.hasNext()) {
                FemStoredColumn column = (FemStoredColumn) iter.next();
                CwmColumnImpl.validateCommon(validator,column);
            }
        }
        
        FarragoMedColumnSet columnSet =
            dataServer.validateColumnSet(validator,this);

        List columnList = getFeature();
        if (columnList.isEmpty()) {
            // derive column information
            SaffronType rowType = columnSet.getRowType();
            int n = rowType.getFieldCount();
            SaffronField [] fields = rowType.getFields();
            for (int i = 0; i < n; ++i) {
                CwmColumn column = catalog.newFemStoredColumn();
                columnList.add(column);
                typeFactory.convertFieldToCwmColumn(fields[i],column);
                CwmColumnImpl.validateCommon(validator,column);
            }
        }
    }

    // implement DdlValidatedElement
    public void validateDeletion(DdlValidator validator,boolean truncation)
    {
    }
}

// End FemForeignTableImpl.java

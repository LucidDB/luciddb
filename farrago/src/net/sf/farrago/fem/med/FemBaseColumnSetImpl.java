/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
// Copyright (C) 2003-2004 Disruptive Tech
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
import net.sf.farrago.type.FarragoTypeFactory;
import net.sf.farrago.namespace.FarragoMedColumnSet;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeField;

import org.netbeans.mdr.handlers.*;
import org.netbeans.mdr.storagemodel.*;

import java.util.*;


/**
 * FemBaseColumnSetImpl is a custom implementation for FemBaseColumnSet
 *
 * @author Kinkoi Lo
 * @version $Id$
 */
public abstract class FemBaseColumnSetImpl extends InstanceHandler
    implements FemBaseColumnSet, DdlValidatedElement
{

    /**
     * Creates a new FemLocalTableImpl object.
     *
     * @param storable .
     */
    protected FemBaseColumnSetImpl(StorableObject storable)
    {
        super(storable);
    }

    //~ Methods ---------------------------------------------------------------

    // implement DdlValidatedElement
    public void validateDefinition(DdlValidator validator, boolean creation) 
    {
        if (getServer().getWrapper().isForeign()) {
            // It is a foreign table
            if (!creation) {
                return;
            }

            FarragoCatalog catalog = validator.getCatalog();
            FarragoTypeFactory typeFactory = validator.getTypeFactory();

            FemDataServerImpl dataServer = (FemDataServerImpl) getServer();
            FemDataWrapper dataWrapper = dataServer.getWrapper();
            if (!dataWrapper.isForeign()) {
                throw validator.res.newValidatorForeignTableButLocalWrapper(
                        catalog.getLocalizedObjectName(this, null),
                        catalog.getLocalizedObjectName(dataWrapper, null));
            }

            validateCommon(validator);

            FarragoMedColumnSet columnSet =
                    dataServer.validateColumnSet(validator, this);

            List columnList = getFeature();
            if (columnList.isEmpty()) {
                // derive column information
                RelDataType rowType = columnSet.getRowType();
                int n = rowType.getFieldCount();
                RelDataTypeField[] fields = rowType.getFields();
                for (int i = 0; i < n; ++i) {
                    CwmColumn column = catalog.newFemStoredColumn();
                    columnList.add(column);
                    typeFactory.convertFieldToCwmColumn(fields[i], column);
                    CwmColumnImpl.validateCommon(validator, column);
                }
            }

        } else {
            validateCommon(validator);
        }
    }


    private void validateCommon(DdlValidator validator)
    {
        validator.validateUniqueNames(this, getFeature(), false);

        if (!getFeature().isEmpty()) {
            // columns were specified; we are to validate them
            Iterator iter = getFeature().iterator();
            while (iter.hasNext()) {
                FemStoredColumn column = (FemStoredColumn) iter.next();
                CwmColumnImpl.validateCommon(validator, column);
            }
        }

        //Foreign tables should not support constraint definitions.  Eventually we
        //may want to allow this as a hint to the optimizer, but it's not standard
        //so for now we should prevent it.
        Iterator constraintIter = getOwnedElement().iterator();
        while (constraintIter.hasNext()) {
            Object obj = constraintIter.next();
            if (!(obj instanceof CwmUniqueConstraint)) {
                continue;
            }
            throw validator.res.newValidatorNoConstrainAllow(
                    getLocalizedName(validator, this));
        }
    }

    private String getLocalizedName(DdlValidator validator, FemBaseColumnSetImpl femBaseColumnSet)
    {
        return validator.getCatalog().getLocalizedObjectName(
                null,
                femBaseColumnSet.getName(),
                femBaseColumnSet.refClass());
    }


    // implement DdlValidatedElement
    public void validateDeletion(DdlValidator validator,boolean truncation)
    {
    }

}
// FemBaseColumnSetImp.java


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
    public void validateDefinition(DdlValidator validator,boolean creation)
    {
        // need to validate columns first
        Iterator columnIter = getFeature().iterator();
        while (columnIter.hasNext()) {
            CwmColumnImpl column = (CwmColumnImpl) columnIter.next();
            column.validateDefinitionImpl(validator);
        }
        
    }
}
// FemBaseColumnSetImp.java


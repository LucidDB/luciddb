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

import net.sf.farrago.util.*;
import net.sf.farrago.type.*;
import net.sf.farrago.session.*;

import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;

import java.util.*;

/**
 * FarragoExecutableStmtImpl is an abstract base for implementations of
 * FarragoSessionExecutableStmt.
 *
 * @author John V. Sichi
 * @version $Id$
 */
abstract class FarragoExecutableStmtImpl
    extends FarragoCompoundAllocation
    implements FarragoSessionExecutableStmt
{
    private final boolean isDml;

    private RelDataType dynamicParamRowType;

    protected FarragoExecutableStmtImpl(
        RelDataType dynamicParamRowType,
        boolean isDml)
    {
        this.isDml = isDml;
        this.dynamicParamRowType = forgetTypeFactory(dynamicParamRowType);
    }

    // implement FarragoSessionExecutableStmt
    public boolean isDml()
    {
        return isDml;
    }

    // implement FarragoSessionExecutableStmt
    public RelDataType getDynamicParamRowType()
    {
        return dynamicParamRowType;
    }

    // implement FarragoSessionExecutableStmt
    public Set getReferencedObjectIds()
    {
        return Collections.EMPTY_SET;
    }
    
    protected static RelDataType forgetTypeFactory(
        RelDataType rowType)
    {
        // Need to forget about the type factory that was used during stmt
        // preparation so that it can be garbage collected.  So, create a
        // private type factory here and use it to create a copy of the row
        // type.

        // TODO:  a better solution would be to serialize this in the form of
        // ResultSetMetaData.  This would (a) guarantee no references; (b) allow
        // for an accurate memory usage computation and (c) allow for
        // persistent caching.  Use RmiJdbc serialization support?
        
        RelDataTypeFactory newTypeFactory = new RelDataTypeFactoryImpl();

        final RelDataTypeField [] fields = rowType.getFields();

        for (int i = 0; i < fields.length; ++i) {
            // FIXME:  get rid of this once all types are guaranteed to be
            // FarragoTypes
            if (!(fields[i].getType() instanceof FarragoType)) {
                continue;
            }
            
            FarragoType farragoType = (FarragoType) fields[i].getType();
            farragoType.forgetFactory();
        }
        
        return newTypeFactory.createProjectType(
            new RelDataTypeFactory.FieldInfo() 
            {
                public int getFieldCount()
                {
                    return fields.length;
                }

                public String getFieldName(int index)
                {
                    return fields[index].getName();
                }

                public RelDataType getFieldType(int index)
                {
                    return fields[index].getType();
                }
            });
    }
}

// End FarragoExecutableStmtImpl.java

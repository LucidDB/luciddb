/*
// $Id$
// Package org.eigenbase is a class library of database components.
// Copyright (C) 2004-2004 Disruptive Tech
// Copyright (C) 2004-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
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
package org.eigenbase.sql.type;

import org.eigenbase.reltype.*;
import org.eigenbase.util.*;

/**
 * MultisetSqlType represents a standard SQL2003 multiset type.
 *
 * @author wael
 * @version $Id$
 */
public class MultisetSqlType extends AbstractSqlType
{
    private RelDataType elementType;

    /**
     * This should only be called from a factory method.
     *
     * @pre null!=elementType
     */
    public MultisetSqlType(RelDataType elementType, boolean isNullable)
    {
        super(SqlTypeName.Multiset, isNullable);
        Util.pre(null!=elementType,"null!=elementType");
        this.elementType = elementType;
        computeDigest();
    }

    // implement RelDataTypeImpl
    protected void generateTypeString(StringBuffer sb, boolean withDetail)
    {
        if (withDetail) {
            sb.append(elementType.getFullTypeString());
        } else {
            sb.append(elementType.toString());
        }
        sb.append(" MULTISET");
    }
    
    // implement RelDataType
    public RelDataType getComponentType()
    {
        return elementType;
    }

    // implement RelDataType
    public RelDataTypeFamily getFamily()
    {
        // TODO jvs 2-Dec-2004:  This gives each multiset type its
        // own family.  But that's not quite correct; the family should
        // be based on the element type for proper comparability
        // semantics (per 4.10.4 of SQL/2003).  So either this should
        // make up canonical families dynamically, or the
        // comparison type-checking should not rely on this.  I
        // think the same goes for ROW types.
        return this;
    }
}

// End MultisetSqlType.java

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
import org.eigenbase.sql.*;

/**
 * ObjectSqlType represents an SQL structured user-defined type.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class ObjectSqlType extends AbstractSqlType
{
    private final SqlIdentifier sqlIdentifier;

    private RelDataTypeFamily family;
    
    /**
     * Constructs an object type.
     * This should only be called from a factory method.
     *
     * @param typeName SqlTypeName for this type (either Distinct or Structured)
     *
     * @param sqlIdentifier identifier for this type
     *
     * @param nullable whether type accepts nulls
     *
     * @param fields object attribute definitions
     */
    public ObjectSqlType(
        SqlTypeName typeName,
        SqlIdentifier sqlIdentifier,
        boolean nullable,
        RelDataTypeField [] fields)
    {
        super(typeName, nullable, fields);
        this.sqlIdentifier = sqlIdentifier;
        computeDigest();
    }

    public void setFamily(RelDataTypeFamily family)
    {
        this.family = family;
    }
    
    // override AbstractSqlType
    public SqlIdentifier getSqlIdentifier()
    {
        return sqlIdentifier;
    }
    
    // override AbstractSqlType
    public RelDataTypeFamily getFamily()
    {
        // each UDT is in its own lonely family, until one day when
        // we support inheritance (at which time also need to implement
        // getPrecedenceList).
        return family;
    }

    // implement RelDataTypeImpl
    protected void generateTypeString(StringBuffer sb, boolean withDetail)
    {
        // TODO jvs 10-Feb-2005:  proper quoting; dump attributes withDetail?
        sb.append("ObjectSqlType(");
        sb.append(sqlIdentifier.toString());
        sb.append(")");
    }
}

// End ObjectSqlType.java

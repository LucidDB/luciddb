/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2004-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2004-2005 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by the Free
// Software Foundation; either version 2 of the License, or (at your option)
// any later version approved by The Eigenbase Project.
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
package org.eigenbase.reltype;

import org.eigenbase.sql.type.*;


/**
 * RelRecordType represents a structured type having named fields.
 *
 * @author jhyde
 * @version $Id$
 */
public class RelRecordType
    extends RelDataTypeImpl
{

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a <code>RecordType</code>. This should only be called from a
     * factory method.
     */
    public RelRecordType(RelDataTypeField [] fields)
    {
        super(fields);
        computeDigest();
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelDataType
    public SqlTypeName getSqlTypeName()
    {
        return SqlTypeName.Row;
    }

    // implement RelDataType
    public boolean isNullable()
    {
        return false;
    }

    // implement RelDataType
    public int getPrecision()
    {
        // REVIEW: angel 18-Aug-2005 Put in fake implementation for precision
        return 0;
    }

    protected void generateTypeString(StringBuilder sb, boolean withDetail)
    {
        sb.append("RecordType(");
        for (int i = 0; i < fields.length; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            RelDataTypeField field = fields[i];
            if (withDetail) {
                sb.append(field.getType().getFullTypeString());
            } else {
                sb.append(field.getType().toString());
            }
            sb.append(" ");
            sb.append(field.getName());
        }
        sb.append(")");
    }
}

// End RelRecordType.java

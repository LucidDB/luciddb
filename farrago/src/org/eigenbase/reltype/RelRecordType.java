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
package org.eigenbase.reltype;

/**
 * RelRecordType represents a structured type having named fields.
 *
 * @author jhyde
 * @version $Id$
 */
public class RelRecordType extends RelDataTypeImpl
{
    /**
     * Creates a <code>RecordType</code>.  This should only be called
     * from a factory method.
     *
     * <p>
     *
     * REVIEW jvs 17-Dec-2004:  is the misspelled comment below correct?
     *
     * <p>
     * 
     * Field names doesnt need to be unique.
     */
    public RelRecordType(RelDataTypeField [] fields)
    {
        super(fields);
        computeDigest();
    }

    public boolean isNullable()
    {
        // REVIEW:  maybe shouldn't even bother.  SQL structured types have
        // a nullable status independent of that of their fields.
        for (int i = 0; i < fields.length; ++i) {
            if (fields[i].getType().isNullable()) {
                return true;
            }
        }
        return false;
    }

    protected void generateTypeString(StringBuffer sb, boolean withDetail)
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

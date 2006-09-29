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

import org.eigenbase.oj.util.*;


// REVIEW jvs 17-Dec-2004:  does this still need to exist?  Is it supposed
// to have fields?

/**
 * Type of the cartesian product of two or more sets of records.
 *
 * <p>Its fields are those of its constituent records, but unlike a {@link
 * RelRecordType}, those fields' names are not necessarily distinct.</p>
 *
 * @author jhyde
 * @version $Id$
 */
public class RelCrossType
    extends RelDataTypeImpl
{

    //~ Instance fields --------------------------------------------------------

    public final RelDataType [] types;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a cartesian product type. This should only be called from a
     * factory method.
     *
     * @pre types != null
     * @pre types.length >= 1
     * @pre !(types[i] instanceof CrossType)
     */
    public RelCrossType(
        RelDataType [] types,
        RelDataTypeField [] fields)
    {
        super(fields);
        this.types = types;
        assert (types != null);
        assert (types.length >= 1);
        for (int i = 0; i < types.length; i++) {
            assert (!(types[i] instanceof RelCrossType));
        }
        computeDigest();
    }

    //~ Methods ----------------------------------------------------------------

    public boolean isStruct()
    {
        return false;
    }

    public RelDataTypeField getField(String fieldName)
    {
        throw new UnsupportedOperationException(
            "not applicable to a join type");
    }

    public int getFieldOrdinal(String fieldName)
    {
        final int ordinal = OJSyntheticClass.getOrdinal(fieldName, false);
        if (ordinal >= 0) {
            return ordinal;
        }
        throw new UnsupportedOperationException(
            "not applicable to a join type");
    }

    public RelDataTypeField [] getFields()
    {
        throw new UnsupportedOperationException(
            "not applicable to a join type");
    }

    public RelDataType [] getTypes()
    {
        return types;
    }

    protected void generateTypeString(StringBuilder sb, boolean withDetail)
    {
        sb.append("CrossType(");
        for (int i = 0; i < types.length; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            RelDataType type = types[i];
            if (withDetail) {
                sb.append(type.getFullTypeString());
            } else {
                sb.append(type.toString());
            }
        }
        sb.append(")");
    }
}

// End RelCrossType.java

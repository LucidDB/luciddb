/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2004-2007 SQLstream, Inc.
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2004-2007 John V. Sichi
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

import java.io.*;

import org.eigenbase.sql.type.*;


/**
 * RelRecordType represents a structured type having named fields.
 *
 * @author jhyde
 * @version $Id$
 */
public class RelRecordType
    extends RelDataTypeImpl
    implements Serializable
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
        return SqlTypeName.ROW;
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

    /**
     * Per {@link Serializable} API, provides a replacement object to be written
     * during serialization.
     *
     * <p>This implementation converts this RelRecordType into a
     * SerializableRelRecordType, whose <code>readResolve</code> method converts
     * it back to a RelRecordType during deserialization.
     */
    private Object writeReplace()
    {
        return new SerializableRelRecordType(fields);
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * Skinny object which has the same information content as a {@link
     * RelRecordType} but skips redundant stuff like digest and the immutable
     * list.
     */
    private static class SerializableRelRecordType
        implements Serializable
    {
        private RelDataTypeField [] fields;

        private SerializableRelRecordType(RelDataTypeField [] fields)
        {
            this.fields = fields;
        }

        /**
         * Per {@link Serializable} API. See {@link
         * RelRecordType#writeReplace()}.
         */
        private Object readResolve()
        {
            return new RelRecordType(fields);
        }
    }
}

// End RelRecordType.java

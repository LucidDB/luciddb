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
package net.sf.farrago.type;

import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.sql.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.resource.*;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.SqlCollation;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.Util;


/**
 * FarragoAtomicType is a refinement of FarragoType representing types whose
 * instances are atomic stored values.
 *
 * @author John V. Sichi
 * @version $Id$
 */
abstract class FarragoAtomicType extends FarragoType
{
    //~ Instance fields -------------------------------------------------------

    private final CwmSqlsimpleType simpleType;
    private final boolean isNullable;

    /** One-element array containing the "this" field. */
    private RelDataTypeField [] fields = new RelDataTypeField[1];

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FarragoAtomicType object.
     *
     * @param simpleType catalog definition for SQL type
     * @param isNullable false if NOT NULL
     * @pre simpleType != null
     */
    protected FarragoAtomicType(
        CwmSqlsimpleType simpleType,
        boolean isNullable)
    {
        Util.pre(simpleType != null, "simpleType != null");
        this.simpleType = simpleType;
        this.isNullable = isNullable;
        fields[0] =
            new RelDataTypeFieldImpl("this", 0, this);
    }

    //~ Methods ---------------------------------------------------------------

    // implement RelDataType
    public RelDataTypeField getField(String fieldName)
    {
        if (getFieldOrdinal(fieldName) == 0) {
            return fields[0];
        } else {
            return null;
        }
    }

    // implement RelDataType
    public int getFieldCount()
    {
        return 1;
    }

    // implement RelDataType
    public int getFieldOrdinal(String fieldName)
    {
        if (fieldName.equals(fields[0].getName())) {
            return 0;
        } else {
            return -1;
        }
    }

    // implement RelDataType
    public RelDataTypeField [] getFields()
    {
        return fields;
    }

    // implement RelDataType
    public RelDataType getComponentType()
    {
        return null; // this is not an array type
    }

    protected void computeDigest()
    {
        StringBuffer sb = new StringBuffer();
        generateTypeString(sb, true);
        if (!isNullable) {
            sb.append(" NOT NULL");
        }
        digest = sb.toString();
    }

    // implement Object
    public String toString()
    {
        StringBuffer sb = new StringBuffer();
        generateTypeString(sb, false);
        return sb.toString();
    }

    // implement RelDataType
    public String getFullTypeString()
    {
        return digest;
    }

    protected void generateTypeString(
        StringBuffer sb,
        boolean withDetail)
    {
        sb.append(simpleType.getName());
        if (takesPrecision()) {
            sb.append('(');
            sb.append(getPrecision());
            if (takesScale() && (getScale() != 0)) {
                sb.append(',');
                sb.append(getScale());
            }
            sb.append(')');
        }
    }

    /**
     * .
     *
     * @return the CwmSqlsimpletype from which this atomic type derives
     */
    protected CwmSqlsimpleType getSimpleType()
    {
        return simpleType;
    }

    // implement RelDataType
    public boolean isNullable()
    {
        return isNullable;
    }

    // implement RelDataType
    public RelDataTypeFamily getFamily()
    {
        return SqlTypeFamily.getFamilyForJdbcType(
            simpleType.getTypeNumber().intValue());
    }

    /**
     * .
     *
     * @return true if this type takes a precision
     */
    protected boolean takesPrecision()
    {
        return getSqlTypeName().allowsPrecScale(true, false);
    }

    /**
     * .
     *
     * @return true if this type takes a scale
     */
    protected boolean takesScale()
    {
        return getSqlTypeName().allowsPrecScale(true, true);
    }

    // implement RelDataType
    public Charset getCharset()
    {
        throw Util.newInternal(digest + " is not defined to carry a charset");
    }

    // implement RelDataType
    public SqlCollation getCollation()
        throws RuntimeException
    {
        throw Util.newInternal(digest + " is not defined to carry a collation");
    }

    // implement RelDataType
    public int getPrecision()
    {
        switch (simpleType.getTypeNumber().intValue()) {
        case Types.BOOLEAN:
        case Types.BIT:
            return 1;
        case Types.TINYINT:
            return 3;
        case Types.SMALLINT:
            return 5;
        case Types.INTEGER:
            return 10;
        case Types.BIGINT:
            return 20;
        }
        throw new AssertionError();
    }

    // implement RelDataType
    public int getScale()
    {
        throw new AssertionError();
    }

    /**
     * @return true when value field access is required at runtime
     */
    protected abstract boolean requiresValueAccess();

    /**
     *  To be overriden by classes with a Primitive rep.
     */
    protected boolean hasClassForPrimitive()
    {
        return false;
    }

    /**
     *
     * @return class for primitive rep.
     */
    protected Class getClassForPrimitive()
    {
        assert (hasClassForPrimitive())
            : "Atomic Type does not have primitive representation";
        return null;
    }

    // implement RelDataType
    public SqlTypeName getSqlTypeName()
    {
        return SqlTypeName.get(this.simpleType.getName());
    }
}


// End FarragoAtomicType.java

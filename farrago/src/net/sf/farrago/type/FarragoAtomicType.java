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

package net.sf.farrago.type;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.resource.*;

import net.sf.saffron.core.*;
import net.sf.saffron.util.Util;
import net.sf.saffron.sql.SqlCollation;

import java.sql.*;
import java.io.PrintWriter;
import java.nio.charset.Charset;


/**
 * FarragoAtomicType is a refinement of FarragoType representing types whose
 * instances are atomic stored values.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class FarragoAtomicType extends FarragoType
{
    //~ Instance fields -------------------------------------------------------

    private final CwmSqlsimpleType simpleType;

    private final boolean isNullable;

    /** One-element array containing the "this" field. */
    private SaffronField [] fields = new SaffronField[1];

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FarragoAtomicType object.
     *
     * @param simpleType catalog definition for SQL type
     * @param isNullable false if NOT NULL
     */
    protected FarragoAtomicType(
        CwmSqlsimpleType simpleType,
        boolean isNullable)
    {
        this.simpleType = simpleType;
        this.isNullable = isNullable;
        fields[0] = new FarragoTypeFactoryImpl.ExposedFieldImpl(
            "this", 0, this);
    }

    //~ Methods ---------------------------------------------------------------

    // implement Type
    public SaffronField getField(String fieldName)
    {
        if (getFieldOrdinal(fieldName) == 0) {
            return fields[0];
        } else {
            return null;
        }
    }

    // implement Type
    public int getFieldCount()
    {
        return 1;
    }

    // implement Type
    public int getFieldOrdinal(String fieldName)
    {
        if (fieldName.equals(fields[0].getName())) {
            return 0;
        } else {
            return -1;
        }
    }

    // implement Type
    public SaffronField [] getFields()
    {
        return fields;
    }

    public SaffronType getArrayType()
    {
        throw Util.needToImplement(this);
    }

    public SaffronType getComponentType()
    {
        return null; // this is not an array type
    }

    public void format(Object value, PrintWriter pw)
    {
        pw.print(value);
    }

    public boolean equalsSansNullability(SaffronType type)
    {
        throw Util.needToImplement(this);
    }

    /**
     * .
     *
     * @return the Sqlsimpletype from which this atomic type derives
     */
    public CwmSqlsimpleType getSimpleType()
    {
        return simpleType;
    }

    // implement SaffronType
    public boolean isNullable()
    {
        return isNullable;
    }

    public boolean isAssignableFrom(SaffronType t)
    {
        // TODO jvs 22-Jan-2004:  implement real SQL rules
        return isSameTypeFamily(t);
    }

    /**
     * .
     *
     * @return the family for this type
     */
    public FarragoTypeFamily getFamily()
    {
        return FarragoTypeFamily.getFamilyForJdbcType(
            simpleType.getTypeNumber().intValue());
    }

    /**
     * .
     *
     * @return true if this type takes a precision
     */
    public boolean takesPrecision()
    {
        switch (simpleType.getTypeNumber().intValue()) {
        case Types.NUMERIC:
        case Types.DECIMAL:
        case Types.TIME:
        case Types.TIMESTAMP:
        case Types.VARBINARY:
        case Types.VARCHAR:
        case Types.BINARY:
        case Types.CHAR:
            return true;
        }
        return false;
    }

    /**
     * .
     *
     * @return true if this type takes a scale
     */
    public boolean takesScale()
    {
        switch (simpleType.getTypeNumber().intValue()) {
        case Types.NUMERIC:
        case Types.DECIMAL:
            return true;
        }
        return false;
    }

    /**
     * .
     *
     * @return true if this type is a large object
     */
    public boolean isLob()
    {
        switch (simpleType.getTypeNumber().intValue()) {
        case Types.BLOB:
        case Types.CLOB:
        case Types.LONGVARCHAR:
        case Types.LONGVARBINARY:
            return true;
        }
        return false;
    }

    /**
     * .
     *
     * @return true if this type is a character or binary string
     */
    public boolean isString()
    {
        FarragoTypeFamily family = getFamily();
        return (family == FarragoTypeFamily.CHARACTER)
            || (family == FarragoTypeFamily.BINARY);
    }

    /** implement SaffronType */
    public boolean isCharType() {
        FarragoTypeFamily family = getFamily();
        return (family == FarragoTypeFamily.CHARACTER);
    }

    /** implement SaffronType */
    public Charset getCharset() {
//        return null;
        throw Util.newInternal(digest+" is not defined to carry a charset");
    }

    /** implement SaffronType */
    public void setCharset(Charset charset) {
        throw Util.newInternal(digest+" is not defined to carry a charset");
    }

    /** implement SaffronType */
    public SqlCollation getCollation() throws RuntimeException {
//        return null;
        throw Util.newInternal(digest+" is not defined to carry a collation");
    }

    /** implement SaffronType */
    public void setCollation(SqlCollation collation) throws RuntimeException {
        throw Util.newInternal(digest+" is not defined to carry a collation");
    }

    /**
     * .
     *
     * @return true if this type is variable width with bounded precision
     */
    public boolean isBoundedVariableWidth()
    {
        // REVIEW:  include NUMERIC/TIME/TIMESTAMP?
        switch (simpleType.getTypeNumber().intValue()) {
        case Types.VARCHAR:
        case Types.VARBINARY:
            return true;
        }
        return false;
    }

    public boolean isExactNumeric()
    {
        switch (simpleType.getTypeNumber().intValue()) {
        case Types.BIT:
        case Types.TINYINT:
        case Types.SMALLINT:
        case Types.INTEGER:
        case Types.BIGINT:
        case Types.NUMERIC:
        case Types.DECIMAL:
            return true;
        }
        return false;
    }
    
    public boolean isApproximateNumeric()
    {
        switch (simpleType.getTypeNumber().intValue()) {
        case Types.FLOAT:
        case Types.REAL:
        case Types.DOUBLE:
            return true;
        }
        return false;
    }
    
    // implement SaffronType
    public boolean isSameTypeFamily(SaffronType other)
    {
        if (!(other instanceof FarragoAtomicType)) {
            return false;
        }
        FarragoAtomicType farragoOther = (FarragoAtomicType) other;
        return getFamily().equals(farragoOther.getFamily());
    }

    /**
     * .
     *
     * @return number of digits or characters of precision
     */
    public int getPrecision()
    {
        switch (simpleType.getTypeNumber().intValue()) {
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

    /**
     * .
     *
     * @return number of digits of scale
     */
    public int getScale()
    {
        throw new AssertionError();
    }
}


// End FarragoAtomicType.java

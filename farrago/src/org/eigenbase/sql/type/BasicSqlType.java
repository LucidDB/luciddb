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
package org.eigenbase.sql.type;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.util.*;

import java.nio.charset.*;

/**
 * BasicSqlType represents a standard atomic SQL type (excluding
 * interval types).
 *
 * @author jhyde
 * @version $Id$
 */
public class BasicSqlType extends AbstractSqlType
{
    public static final int SCALE_NOT_SPECIFIED = Integer.MIN_VALUE;
    public static final int PRECISION_NOT_SPECIFIED = -1;
    private final int precision;
    private final int scale;
    private SqlCollation collation;
    private Charset charset;

    /**
     * Constructs a type with no parameters.
     * This should only be called from a factory method.
     *
     * @param typeName Type name
     * @pre typeName.allowsNoPrecNoScale(false,false)
     */
    public BasicSqlType(SqlTypeName typeName)
    {
        super(typeName, false, null);
        Util.pre(
            typeName.allowsPrecScale(false, false),
            "typeName.allowsPrecScale(false,false), typeName="
            + typeName.getName());
        this.precision = PRECISION_NOT_SPECIFIED;
        this.scale = SCALE_NOT_SPECIFIED;
        computeDigest();
    }

    /**
     * Constructs a type with precision/length but no scale.
     * @param typeName Type name
     * @pre typeName.allowsPrecNoScale(true,false)
     */
    public BasicSqlType(
        SqlTypeName typeName,
        int precision)
    {
        super(typeName, false, null);
        Util.pre(
            typeName.allowsPrecScale(true, false),
            "typeName.allowsPrecScale(true,false)");
        this.precision = precision;
        this.scale = SCALE_NOT_SPECIFIED;
        computeDigest();
    }

    /**
     * Constructs a type with precision/length and scale.
     * @param typeName Type name
     * @pre typeName.allowsPrecScale(true,true)
     */
    public BasicSqlType(
        SqlTypeName typeName,
        int precision,
        int scale)
    {
        super(typeName, false, null);
        Util.pre(
            typeName.allowsPrecScale(true, true),
            "typeName.allowsPrecScale(true,true)");
        this.precision = precision;
        this.scale = scale;
        computeDigest();
    }

    /**
     * Constructs a type with nullablity
     */
    BasicSqlType createWithNullability(boolean nullable)
    {
        BasicSqlType ret = null;
        try {
            ret = (BasicSqlType) this.clone();
        } catch (CloneNotSupportedException e) {
            throw Util.newInternal(e);
        }
        ret.isNullable = nullable;
        ret.computeDigest();
        return ret;
    }

    /**
     * Constructs a type with charset and collation
     * @pre SqlTypeUtil.inCharFamily(this)
     */
    BasicSqlType createWithCharsetAndCollation(
        Charset charset,
        SqlCollation collation)
    {
        Util.pre(SqlTypeUtil.inCharFamily(this) == true, "Not an chartype");
        BasicSqlType ret;
        try {
            ret = (BasicSqlType) this.clone();
        } catch (CloneNotSupportedException e) {
            throw Util.newInternal(e);
        }
        ret.charset = charset;
        ret.collation = collation;
        ret.computeDigest();
        return ret;
    }

    //implement RelDataType
    public int getPrecision()
    {
        if (precision == PRECISION_NOT_SPECIFIED) {
            switch (typeName.getOrdinal()) {
            case SqlTypeName.Boolean_ordinal:
                return 1;
            case SqlTypeName.Tinyint_ordinal:
                return 3;
            case SqlTypeName.Smallint_ordinal:
                return 5;
            case SqlTypeName.Integer_ordinal:
                return 10;
            case SqlTypeName.Bigint_ordinal:
                return 19;
            case SqlTypeName.Decimal_ordinal:
                return SqlTypeName.MAX_NUMERIC_PRECISION;
            case SqlTypeName.Real_ordinal:
                return 7;
            case SqlTypeName.Float_ordinal:
            case SqlTypeName.Double_ordinal:
                return 15;
            case SqlTypeName.Time_ordinal:
                return 0;               // SQL99 part 2 section 6.1 syntax rule 30
            case SqlTypeName.Timestamp_ordinal:
                // farrago supports only 0 (see SqlTypeName.getDefaultPrecision),
                // but it should be 6 (microseconds) per SQL99 part 2 section 6.1 syntax rule 30.
                return 0;  
            default:
                throw Util.newInternal("type "+typeName+" does not have a precision");
            }
        }
        return precision;
    }

    // implement RelDataType
    public int getScale()
    {
        if (scale == SCALE_NOT_SPECIFIED) {
            switch (typeName.getOrdinal()) {
            case SqlTypeName.Tinyint_ordinal:
            case SqlTypeName.Smallint_ordinal:
            case SqlTypeName.Integer_ordinal:
            case SqlTypeName.Bigint_ordinal:
            case SqlTypeName.Decimal_ordinal:
                return 0;
            default:
                throw Util.newInternal("type "+typeName+" does not have a scale");
            }
        }
        return scale;
    }

    // implement RelDataType
    public Charset getCharset()
        throws RuntimeException
    {
        return charset;
    }

    // implement RelDataType
    public SqlCollation getCollation()
        throws RuntimeException
    {
        return collation;
    }


    // implement RelDataTypeImpl
    protected void generateTypeString(StringBuffer sb, boolean withDetail)
    {
        // Called to make the digest, which equals() compares;
        // so equivalent data types must produce identical type strings.

        sb.append(typeName.getName());
        boolean printPrecision = (precision != PRECISION_NOT_SPECIFIED);
        boolean printScale = (scale != SCALE_NOT_SPECIFIED);

        // for the digest, print the precision when defaulted,
        // since (for instance) TIME is equivalent to TIME(0).
        if (withDetail) {
            // -1 means there is no default value for precision
            if (typeName.getDefaultPrecision() > -1) {
                printPrecision = true;
            }
            if (typeName.getDefaultScale() > -1) {
                printScale = true;
            }
        }

        if (printPrecision) {
            sb.append('(');
            sb.append(getPrecision());
            if (printScale) {
                sb.append(", ");
                sb.append(getScale());
            }
            sb.append(')');
        }
        if (!withDetail) {
            return;
        }
        if (charset != null) {
            sb.append(" CHARACTER SET \"");
            sb.append(charset.name());
            sb.append("\"");
        }
        if (collation != null) {
            sb.append(" COLLATE \"");
            sb.append(collation.getCollationName());
            sb.append("\"");
        }
    }
}

// End BasicSqlType.java

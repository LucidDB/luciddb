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
import org.eigenbase.util.*;

import java.nio.charset.*;

/**
 * BasicSqlType represents a standard atomic SQL type (excluding
 * interval types).
 *
 * @author John V. Sichi
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
     * @param typeName Type name
     * @pre typeName.allowsNoPrecNoScale(false,false)
     */
    public BasicSqlType(SqlTypeName typeName)
    {
        super(typeName, true);
        Util.pre(
            typeName.allowsPrecScale(false, false),
            "typeName.allowsPrecScale(false,false), typeName="
            + typeName.name);
        this.precision = PRECISION_NOT_SPECIFIED;
        this.scale = SCALE_NOT_SPECIFIED;
        this.digest = typeName.name;
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
        super(typeName, true);
        Util.pre(
            typeName.allowsPrecScale(true, false),
            "typeName.allowsPrecScale(true,false)");
        this.precision = precision;
        this.scale = SCALE_NOT_SPECIFIED;
        this.digest = typeName.name + "(" + precision + ")";
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
        super(typeName, true);
        Util.pre(
            typeName.allowsPrecScale(true, true),
            "typeName.allowsPrecScale(true,true)");
        this.precision = precision;
        this.scale = scale;
        this.digest =
            typeName.name + "(" + precision + ", " + scale + ")";
    }

    /**
     * Constructs a type with nullablity
     */
    public BasicSqlType createWithNullability(boolean nullable)
    {
        BasicSqlType ret = null;
        try {
            ret = (BasicSqlType) this.clone();
        } catch (CloneNotSupportedException e) {
            throw Util.newInternal(e);
        }
        ret.isNullable = nullable;
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
        return ret;
    }

    //implement RelDataType
    public int getPrecision()
    {
        return precision;
    }

    public Charset getCharset()
        throws RuntimeException
    {
        if (!SqlTypeUtil.inCharFamily(this)) {
            throw Util.newInternal(typeName.toString()
                + " is not defined to carry a charset");
        }
        return this.charset;
    }

    public SqlCollation getCollation()
        throws RuntimeException
    {
        if (!SqlTypeUtil.inCharFamily(this)) {
            throw Util.newInternal(typeName.toString()
                + " is not defined to carry a collation");
        }
        return this.collation;
    }
}

// End BasicSqlType.java

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

import java.math.*;

import java.sql.*;
import java.sql.Date;

import java.util.*;


/**
 * JavaToSqlTypeConversionRules defines mappings from common Java types to
 * corresponding SQL types.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class JavaToSqlTypeConversionRules
{

    //~ Static fields/initializers ---------------------------------------------

    private static final JavaToSqlTypeConversionRules instance =
        new JavaToSqlTypeConversionRules();

    //~ Instance fields --------------------------------------------------------

    private final HashMap rules = new HashMap();

    //~ Constructors -----------------------------------------------------------

    private JavaToSqlTypeConversionRules()
    {
        rules.put(Integer.class, SqlTypeName.Integer);
        rules.put(int.class, SqlTypeName.Integer);
        rules.put(Long.class, SqlTypeName.Bigint);
        rules.put(long.class, SqlTypeName.Bigint);
        rules.put(Short.class, SqlTypeName.Smallint);
        rules.put(short.class, SqlTypeName.Smallint);
        rules.put(byte.class, SqlTypeName.Tinyint);
        rules.put(Byte.class, SqlTypeName.Tinyint);

        rules.put(Float.class, SqlTypeName.Real);
        rules.put(float.class, SqlTypeName.Real);
        rules.put(Double.class, SqlTypeName.Double);
        rules.put(double.class, SqlTypeName.Double);

        rules.put(boolean.class, SqlTypeName.Boolean);
        rules.put(Boolean.class, SqlTypeName.Boolean);
        rules.put(byte [].class, SqlTypeName.Varbinary);
        rules.put(String.class, SqlTypeName.Varchar);
        rules.put(char [].class, SqlTypeName.Varchar);
        rules.put(Character.class, SqlTypeName.Char);
        rules.put(char.class, SqlTypeName.Char);

        rules.put(Date.class, SqlTypeName.Date);
        rules.put(Timestamp.class, SqlTypeName.Timestamp);
        rules.put(Time.class, SqlTypeName.Time);
        rules.put(BigDecimal.class, SqlTypeName.Decimal);

        rules.put(ResultSet.class, SqlTypeName.Cursor);
        rules.put(List.class, SqlTypeName.ColumnList);
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Returns the {@link org.eigenbase.util.Glossary#SingletonPattern
     * singleton} instance.
     */
    public static JavaToSqlTypeConversionRules instance()
    {
        return instance;
    }

    /**
     * Returns a corresponding {@link SqlTypeName} for a given Java class.
     *
     * @param javaClass the Java class to lookup
     *
     * @return a corresponding SqlTypeName if found, otherwise null is returned
     */
    public SqlTypeName lookup(Class javaClass)
    {
        return (SqlTypeName) rules.get(javaClass);
    }
}

// End JavaToSqlTypeConversionRules.java

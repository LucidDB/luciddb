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

    private final Map<Class<?>, SqlTypeName> rules =
        new HashMap<Class<?>, SqlTypeName>();

    //~ Constructors -----------------------------------------------------------

    private JavaToSqlTypeConversionRules()
    {
        rules.put(Integer.class, SqlTypeName.INTEGER);
        rules.put(int.class, SqlTypeName.INTEGER);
        rules.put(Long.class, SqlTypeName.BIGINT);
        rules.put(long.class, SqlTypeName.BIGINT);
        rules.put(Short.class, SqlTypeName.SMALLINT);
        rules.put(short.class, SqlTypeName.SMALLINT);
        rules.put(byte.class, SqlTypeName.TINYINT);
        rules.put(Byte.class, SqlTypeName.TINYINT);

        rules.put(Float.class, SqlTypeName.REAL);
        rules.put(float.class, SqlTypeName.REAL);
        rules.put(Double.class, SqlTypeName.DOUBLE);
        rules.put(double.class, SqlTypeName.DOUBLE);

        rules.put(boolean.class, SqlTypeName.BOOLEAN);
        rules.put(Boolean.class, SqlTypeName.BOOLEAN);
        rules.put(byte [].class, SqlTypeName.VARBINARY);
        rules.put(String.class, SqlTypeName.VARCHAR);
        rules.put(char [].class, SqlTypeName.VARCHAR);
        rules.put(Character.class, SqlTypeName.CHAR);
        rules.put(char.class, SqlTypeName.CHAR);

        rules.put(Date.class, SqlTypeName.DATE);
        rules.put(Timestamp.class, SqlTypeName.TIMESTAMP);
        rules.put(Time.class, SqlTypeName.TIME);
        rules.put(BigDecimal.class, SqlTypeName.DECIMAL);

        rules.put(ResultSet.class, SqlTypeName.CURSOR);
        rules.put(List.class, SqlTypeName.COLUMN_LIST);
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
        return rules.get(javaClass);
    }
}

// End JavaToSqlTypeConversionRules.java

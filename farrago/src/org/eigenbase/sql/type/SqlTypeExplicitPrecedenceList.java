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

import java.util.*;

/**
 * SqlTypeExplicitPrecedenceList implements the
 * {@link RelDataTypePrecedenceList} interface via an explicit list
 * of SqlTypeName entries.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class SqlTypeExplicitPrecedenceList implements RelDataTypePrecedenceList
{
    /**
     * Map from SqlTypeName to corresponding precedence list.
     *
     * @sql.2003 Part 2 Section 9.5
     */
    private static final Map typeNameToPrecedenceList;

    private final List typeNames;

    static 
    {
        // NOTE jvs 25-Jan-2005:  the null entries delimit equivalence
        // classes
        List numericList = Arrays.asList(
            new SqlTypeName [] {
                SqlTypeName.Tinyint,
                null,
                SqlTypeName.Smallint,
                null, 
                SqlTypeName.Integer,
                null, 
                SqlTypeName.Bigint,
                null, 
                SqlTypeName.Decimal,
                null,
                SqlTypeName.Real,
                SqlTypeName.Float,
                null,
                SqlTypeName.Double }
            );
        typeNameToPrecedenceList = new HashMap();
        addList(
            SqlTypeName.Boolean,
            new SqlTypeName [] { SqlTypeName.Boolean });
        addNumericList(
            SqlTypeName.Tinyint,
            numericList);
        addNumericList(
            SqlTypeName.Smallint,
            numericList);
        addNumericList(
            SqlTypeName.Integer,
            numericList);
        addNumericList(
            SqlTypeName.Bigint,
            numericList);
        addNumericList(
            SqlTypeName.Decimal,
            numericList);
        addNumericList(
            SqlTypeName.Real,
            numericList);
        addNumericList(
            SqlTypeName.Float,
            numericList);
        addNumericList(
            SqlTypeName.Double,
            numericList);
        addList(
            SqlTypeName.Bit, 
            new SqlTypeName [] { SqlTypeName.Bit, SqlTypeName.Varbit });
        addList(
            SqlTypeName.Varbit, 
            new SqlTypeName [] { SqlTypeName.Varbit });
        addList(
            SqlTypeName.Char, 
            new SqlTypeName [] { SqlTypeName.Char, SqlTypeName.Varchar });
        addList(
            SqlTypeName.Varchar, 
            new SqlTypeName [] { SqlTypeName.Varchar });
        addList(
            SqlTypeName.Binary, 
            new SqlTypeName [] { SqlTypeName.Binary, SqlTypeName.Varbinary });
        addList(
            SqlTypeName.Varbinary, 
            new SqlTypeName [] { SqlTypeName.Varbinary });
        addList(
            SqlTypeName.Date, 
            new SqlTypeName [] { SqlTypeName.Date });
        addList(
            SqlTypeName.Time, 
            new SqlTypeName [] { SqlTypeName.Time });
        addList(
            SqlTypeName.Timestamp, 
            new SqlTypeName [] { SqlTypeName.Timestamp });
        addList(
            SqlTypeName.IntervalYearMonth, 
            new SqlTypeName [] { SqlTypeName.IntervalYearMonth });
        addList(
            SqlTypeName.IntervalDayTime, 
            new SqlTypeName [] { SqlTypeName.IntervalDayTime });
    }

    private static void addList(
        SqlTypeName typeName, SqlTypeName [] array)
    {
        typeNameToPrecedenceList.put(
            typeName,
            new SqlTypeExplicitPrecedenceList(array));
    }

    private static void addNumericList(
        SqlTypeName typeName, List numericList)
    {
        int i = getListPosition(typeName, numericList);
        SqlTypeName [] array = (SqlTypeName [])
            numericList.subList(i, numericList.size()).toArray(
                SqlTypeName.EMPTY_ARRAY);
        addList(typeName, array);
    }
    
    public SqlTypeExplicitPrecedenceList(SqlTypeName [] typeNames)
    {
        this.typeNames = Arrays.asList(typeNames);
    }

    // implement RelDataTypePrecedenceList
    public boolean containsType(RelDataType type)
    {
        SqlTypeName typeName = type.getSqlTypeName();
        if (typeName == null) {
            return false;
        }
        return typeNames.contains(typeName);
    }
    
    // implement RelDataTypePrecedenceList
    public int compareTypePrecedence(RelDataType type1, RelDataType type2)
    {
        assert(containsType(type1));
        assert(containsType(type2));
        
        int p1 = getListPosition(type1.getSqlTypeName(), typeNames);
        int p2 = getListPosition(type2.getSqlTypeName(), typeNames);
        return p2 - p1;
    }

    private static int getListPosition(SqlTypeName type, List list)
    {
        int i = list.indexOf(type);
        assert(i != -1);

        // adjust for precedence equivalence classes
        for (int j = i - 1; j >= 0; --j) {
            if (list.get(j) == null) {
                return j;
            }
        }
        return i;
    }

    static RelDataTypePrecedenceList getListForType(RelDataType type)
    {
        SqlTypeName typeName = type.getSqlTypeName();
        if (typeName == null) {
            return null;
        }
        return (RelDataTypePrecedenceList)
            typeNameToPrecedenceList.get(typeName);
    }
}

// End SqlTypeExplicitPrecedenceList.java

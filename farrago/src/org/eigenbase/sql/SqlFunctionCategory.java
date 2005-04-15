/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
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
package org.eigenbase.sql;

import org.eigenbase.util.*;

/**
 * <code>SqlFunctionCategory</codde> is an enumeration of the categories
 * of SQL-invoked routines.
 *
 * @version $Id$
 */
public class SqlFunctionCategory extends EnumeratedValues.BasicValue
{
    //~ Static fields/initializers --------------------------------------------

    public static final int String_ordinal = 0;

    /** String function type **/
    public static final SqlFunctionCategory String =
        new SqlFunctionCategory("STRING", String_ordinal, "String function");
        
    public static final int Numeric_ordinal = 1;

    /** Numeric function type **/
    public static final SqlFunctionCategory Numeric =
        new SqlFunctionCategory("NUMERIC", Numeric_ordinal, "Numeric function");
        
    public static final int TimeDate_ordinal = 2;

    /** Time and date function type **/
    public static final SqlFunctionCategory TimeDate =
        new SqlFunctionCategory("TIMEDATE", TimeDate_ordinal,
            "Time and date function");
        
    public static final int System_ordinal = 3;

    /** System function type **/
    public static final SqlFunctionCategory System =
        new SqlFunctionCategory("SYSTEM", System_ordinal, "System function");

    public static final int UserDefinedFunction_ordinal = 4;
        
    /** User-defined function type **/
    public static final SqlFunctionCategory UserDefinedFunction =
        new SqlFunctionCategory(
            "UDF", UserDefinedFunction_ordinal, "User-defined function");

    public static final int UserDefinedProcedure_ordinal = 5;
        
    /** User-defined procedure type **/
    public static final SqlFunctionCategory UserDefinedProcedure =
        new SqlFunctionCategory(
            "UDP", UserDefinedProcedure_ordinal, "User-defined procedure");

    public static final int UserDefinedConstructor_ordinal = 6;
        
    /** User-defined constructor type **/
    public static final SqlFunctionCategory UserDefinedConstructor =
        new SqlFunctionCategory(
            "UDC", UserDefinedConstructor_ordinal,
            "User-defined constructor");

    public static final int UserDefinedSpecificFunction_ordinal = 7;
        
    /** User-defined function type with SPECIFIC name **/
    public static final SqlFunctionCategory UserDefinedSpecificFunction =
        new SqlFunctionCategory(
            "UDF_SPECIFIC", UserDefinedSpecificFunction_ordinal,
            "User-defined function with SPECIFIC name");

    public static final EnumeratedValues enumeration =
        new EnumeratedValues(new SqlFunctionCategory [] {
                String,
                Numeric,
                TimeDate,
                System,
                UserDefinedFunction,
                UserDefinedProcedure,
                UserDefinedConstructor,
                UserDefinedSpecificFunction
            });

    /**
     * Looks up a kind from its ordinal.
     */
    public static SqlFunctionCategory get(int ordinal)
    {
        return (SqlFunctionCategory) enumeration.getValue(ordinal);
    }

    /**
     * Looks up a kind from its name.
     */
    public static SqlFunctionCategory get(String name)
    {
        return (SqlFunctionCategory) enumeration.getValue(name);
    }

    private SqlFunctionCategory(
        String name,
        int ordinal,
        String description)
    {
        super(name, ordinal, description);
    }
}

// End SqlFunctionCategory.java

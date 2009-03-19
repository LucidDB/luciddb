/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2002-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
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
 * <code>SqlFunctionCategory</codde> is an enumeration of the categories of
 * SQL-invoked routines.
 *
 * @version $Id$
 */
public enum SqlFunctionCategory
{
    String("STRING", "String function"), Numeric("NUMERIC", "Numeric function"),
    TimeDate("TIMEDATE", "Time and date function"),
    System("SYSTEM", "System function"),
    UserDefinedFunction("UDF", "User-defined function"),
    UserDefinedProcedure("UDP", "User-defined procedure"),
    UserDefinedConstructor("UDC", "User-defined constructor"),
    UserDefinedSpecificFunction(
        "UDF_SPECIFIC",
        "User-defined function with SPECIFIC name");

    SqlFunctionCategory(
        String abbrev,
        String description)
    {
        Util.discard(abbrev);
        Util.discard(description);
    }
}

// End SqlFunctionCategory.java

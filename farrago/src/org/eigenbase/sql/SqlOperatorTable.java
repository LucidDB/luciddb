/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.sql.parser.SqlParserPos;
import org.eigenbase.util.MultiMap;
import org.eigenbase.util.Util;

import java.lang.reflect.Field;
import java.util.*;


/**
 * SqlOperatorTable defines a directory interface for enumerating and
 * looking up SQL operators and functions.
 */
public interface SqlOperatorTable
{
    /**
     * Retrieves a list of operators with a given name and syntax.  For
     * example, by passing SqlSyntax.Function, the returned list is
     * narrowed to only matching SqlFunction objects.
     *
     * @param opName name of operator
     *
     * @param syntax syntax type of operator
     *
     * @return mutable list of SqlOperator objects
     * (or immutable empty list if no matches)
     */
    public List lookupOperatorOverloads(
        SqlIdentifier opName,
        SqlSyntax syntax);

    /**
     * Retrieves a list of all functions and operators in this table.
     * Used for automated testing.
     *
     * @return list of SqlOperator objects
     */
    public List getOperatorList();
}


// End SqlOperatorTable.java


/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
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
 * Defines the keywords which can occur immediately after the "INSERT" keyword.
 * Standard SQL has no such keywords. This enumeration exists only to allow
 * extension projects to define them.
 */
public class SqlInsertKeyword
    extends EnumeratedValues.BasicValue
    implements SqlLiteral.SqlSymbol
{
    //~ Static fields/initializers ---------------------------------------------

    public static final EnumeratedValues enumeration =
        new EnumeratedValues(new SqlInsertKeyword[] {});

    //~ Constructors -----------------------------------------------------------

    protected SqlInsertKeyword(String name, int ordinal)
    {
        super(name, ordinal, null);
    }

    //~ Methods ----------------------------------------------------------------

    public String name()
    {
        return getName();
    }

    public int ordinal()
    {
        return getOrdinal();
    }
}

// End SqlInsertKeyword.java

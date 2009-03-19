/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
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
 * Defines the keywords which can occur immediately after the "SELECT" keyword.
 *
 * @author jhyde
 * @version $Id$
 */
public class SqlSelectKeyword
    extends EnumeratedValues.BasicValue
    implements SqlLiteral.SqlSymbol
{
    //~ Static fields/initializers ---------------------------------------------

    public static final int Distinct_ordinal = 0;
    public static final SqlSelectKeyword Distinct =
        new SqlSelectKeyword("Distinct", Distinct_ordinal);
    public static final int All_ordinal = 1;
    public static final SqlSelectKeyword All =
        new SqlSelectKeyword("All", All_ordinal);
    public static final EnumeratedValues enumeration =
        new EnumeratedValues(new SqlSelectKeyword[] { Distinct, All });

    //~ Constructors -----------------------------------------------------------

    protected SqlSelectKeyword(String name, int ordinal)
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

// End SqlSelectKeyword.java

/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2004-2005 The Eigenbase Project
// Copyright (C) 2004-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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
package org.eigenbase.sql.validate;

import org.eigenbase.util.*;


/**
 * An enumeration of moniker types. used in {@link SqlMoniker}
 *
 * @author tleung
 * @version $Id$
 * @since May 24, 2005
 */
public class SqlMonikerType
    extends EnumeratedValues.BasicValue
{
    //~ Static fields/initializers ---------------------------------------------

    public static final SqlMonikerType Column = new SqlMonikerType("Column", 0);
    public static final SqlMonikerType Table = new SqlMonikerType("Table", 1);
    public static final SqlMonikerType View = new SqlMonikerType("View", 2);
    public static final SqlMonikerType Schema = new SqlMonikerType("Schema", 3);
    public static final SqlMonikerType Repository =
        new SqlMonikerType("Repository", 4);
    public static final SqlMonikerType Function =
        new SqlMonikerType("Function", 5);

    //~ Constructors -----------------------------------------------------------

    public SqlMonikerType(String name, int ordinal)
    {
        super(name, ordinal, name);
    }
}

// End SqlMonikerType.java

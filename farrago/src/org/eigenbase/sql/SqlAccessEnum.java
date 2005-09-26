/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
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

package org.eigenbase.sql;

import org.eigenbase.util.EnumeratedValues;

/**
 * Enumeration representing different access types
 *
 * @author angel
 * @version $Id$
 * @since Sep 18, 2005
 */
// TODO: Change to use Enum
public class SqlAccessEnum extends EnumeratedValues.BasicValue
{
    //~ Static fields/initializers --------------------------------------------

    public static final int SelectORDINAL = 1;
    public static final SqlAccessEnum SELECT = new SqlAccessEnum("SELECT", SelectORDINAL);

    public static final int UpdateORDINAL = 2;
    public static final SqlAccessEnum UPDATE = new SqlAccessEnum("UPDATE", UpdateORDINAL);

    public static final int InsertORDINAL = 3;
    public static final SqlAccessEnum INSERT = new SqlAccessEnum("INSERT", InsertORDINAL);

    public static final int DeleteORDINAL = 4;
    public static final SqlAccessEnum DELETE = new SqlAccessEnum("DELETE", DeleteORDINAL);


    public static final SqlAccessEnum[] ALL = { SELECT, UPDATE, INSERT, DELETE };
    public static final SqlAccessEnum[] READ_ONLY = { SELECT };
    public static final SqlAccessEnum[] WRITE_ONLY = { INSERT };

    public static final EnumeratedValues enumeration = new EnumeratedValues(ALL);

    //~ Constructors ----------------------------------------------------------

    private SqlAccessEnum(
        String name,
        int ordinal)
    {
        super(name, ordinal, null);
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * Looks up an access enum from its ordinal.
     */
    public static SqlAccessEnum get(int ordinal)
    {
        return (SqlAccessEnum) enumeration.getValue(ordinal);
    }

    /**
     * Looks up an access type from its name.
     */
    public static SqlAccessEnum get(String name)
    {
        return (SqlAccessEnum) enumeration.getValue(name);
    }
}

// End SqlAccessEnum.java

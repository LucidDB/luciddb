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

import java.util.*;


/**
 * SqlAccessType is represented by a set of allowed access types
 *
 * @author angel
 * @version $Id$
 * @since Sep 16, 2005
 */
public class SqlAccessType
{
    //~ Static fields/initializers ---------------------------------------------

    public static final SqlAccessType ALL =
        new SqlAccessType(EnumSet.allOf(SqlAccessEnum.class));
    public static final SqlAccessType READ_ONLY =
        new SqlAccessType(EnumSet.of(SqlAccessEnum.SELECT));
    public static final SqlAccessType WRITE_ONLY =
        new SqlAccessType(EnumSet.of(SqlAccessEnum.INSERT));

    //~ Instance fields --------------------------------------------------------

    private final EnumSet<SqlAccessEnum> accessEnums;

    //~ Constructors -----------------------------------------------------------

    public SqlAccessType(EnumSet<SqlAccessEnum> accessEnums)
    {
        this.accessEnums = accessEnums;
    }

    //~ Methods ----------------------------------------------------------------

    public boolean allowsAccess(SqlAccessEnum access)
    {
        return accessEnums.contains(access);
    }

    public String toString()
    {
        return accessEnums.toString();
    }

    public static SqlAccessType create(String [] accessNames)
    {
        assert accessNames != null;
        EnumSet<SqlAccessEnum> enumSet = EnumSet.noneOf(SqlAccessEnum.class);
        for (int i = 0; i < accessNames.length; i++) {
            enumSet.add(
                SqlAccessEnum.valueOf(
                    accessNames[i].trim().toUpperCase()));
        }
        return new SqlAccessType(enumSet);
    }

    public static SqlAccessType create(String accessString)
    {
        assert accessString != null;
        accessString = accessString.replace('[', ' ');
        accessString = accessString.replace(']', ' ');
        String [] accessNames = accessString.split(",");
        return create(accessNames);
    }
}

// End SqlAccessType.java

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

import org.eigenbase.util.*;


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
        new SqlAccessType(SqlAccessEnum.ALL);
    public static final SqlAccessType READ_ONLY =
        new SqlAccessType(SqlAccessEnum.READ_ONLY);
    public static final SqlAccessType WRITE_ONLY =
        new SqlAccessType(SqlAccessEnum.WRITE_ONLY);

    //~ Instance fields --------------------------------------------------------

    // TODO: Change to use EnumSet
    private EnumeratedValues accessEnums;

    //~ Constructors -----------------------------------------------------------

    public SqlAccessType(SqlAccessEnum [] accessEnums)
    {
        this.accessEnums = new EnumeratedValues(accessEnums);
    }

    //~ Methods ----------------------------------------------------------------

    public boolean allowsAccess(SqlAccessEnum access)
    {
        return accessEnums.containsName(access.getName());
    }

    public String [] getNames()
    {
        return accessEnums.getNames();
    }

    public String toString()
    {
        String [] names = accessEnums.getNames();
        return Arrays.asList(names).toString();
    }

    public static SqlAccessType create(String [] accessNames)
    {
        if (accessNames != null) {
            SqlAccessEnum [] accessEnums =
                new SqlAccessEnum[accessNames.length];
            for (int i = 0; i < accessNames.length; i++) {
                accessEnums[i] =
                    SqlAccessEnum.get(
                        accessNames[i].trim().toUpperCase());
            }
            return new SqlAccessType(accessEnums);
        } else {
            return null;
        }
    }

    public static SqlAccessType create(String accessString)
    {
        if (accessString != null) {
            accessString = accessString.replace('[', ' ');
            accessString = accessString.replace(']', ' ');
            String [] accessNames = accessString.split(",");
            return create(accessNames);
        } else {
            return null;
        }
    }
}

// End SqlAccessType.java

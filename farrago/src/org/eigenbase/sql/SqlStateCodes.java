/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
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
package org.eigenbase.sql;

import org.eigenbase.util.EnumeratedValues;

/**
 * Contains {@link org.eigenbase.util.Glossary#Sql2003} SQL state codes.
 * Sql Sate codes are defined in
 * <pre><code> &#64;sql.2003 Part 2 Section 23.1</code></pre>
 *
 * @author Wael Chatila
 * @since Mar 30, 2005
 * @version $Id$
 */
public class SqlStateCodes  extends EnumeratedValues.BasicValue
{
    private final String stateClass;
    private final String stateSubClass;

    public SqlStateCodes(String name, int ordinal, String stateClass, String stateSubClass)
    {
        super(name, ordinal, null);
        this.stateClass = stateClass;
        this.stateSubClass = stateSubClass;
    }

    public String getStateClass()
    {
        return stateClass;
    }

    public String getStateSubClass()
    {
        return stateSubClass;
    }

    public String getState()
    {
        return stateClass + stateSubClass;
    }

    public static final int CardinalityViolation_ORDINAL = 0;
    public static final SqlStateCodes CardinalityViolation =
        new SqlStateCodes(
            "cardinality violation",
            CardinalityViolation_ORDINAL,
            "21",
            "000");

    public static final int NullValueNotAllowed_ORDINAL = 1;
    public static final SqlStateCodes NullValueNotAllowed =
        new SqlStateCodes(
            "null value not allowed",
            NullValueNotAllowed_ORDINAL,
            "22",
            "004");

}

/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2004-2009 SQLstream, Inc.
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

/**
 * Contains {@link org.eigenbase.util.Glossary#Sql2003} SQL state codes. Sql
 * Sate codes are defined in
 *
 * <pre><code> &#64;sql.2003 Part 2 Section 23.1</code></pre>
 *
 * @author Wael Chatila
 * @version $Id$
 * @since Mar 30, 2005
 */
public enum SqlStateCodes
{
    CardinalityViolation("cardinality violation", "21", "000"),

    NullValueNotAllowed("null value not allowed", "22", "004"),

    NumericValueOutOfRange("numeric value out of range", "22", "003");

    private final String msg;
    private final String stateClass;
    private final String stateSubClass;

    SqlStateCodes(
        String msg,
        String stateClass,
        String stateSubClass)
    {
        this.msg = msg;
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
}

// End SqlStateCodes.java

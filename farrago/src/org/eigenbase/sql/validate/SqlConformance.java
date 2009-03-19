/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2007-2009 The Eigenbase Project
// Copyright (C) 2007-2009 SQLstream, Inc.
// Copyright (C) 2007-2009 LucidEra, Inc.
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

/**
 * Enumeration of valid SQL compatiblity modes.
 *
 * @author jhyde
 * @version $Id$
 * @since Nov 06, 2007
 */
public enum SqlConformance
{
    Default, Strict92, Strict99, Pragmatic99, Oracle10g, Sql2003, Pragmatic2003;

    /**
     * Whether 'order by 2' is interpreted to mean 'sort by the 2nd column in
     * the select list'.
     */
    public boolean isSortByOrdinal()
    {
        switch (this) {
        case Default:
        case Oracle10g:
        case Strict92:
        case Pragmatic99:
        case Pragmatic2003:
            return true;
        default:
            return false;
        }
    }

    /**
     * Whether 'order by x' is interpreted to mean 'sort by the select list item
     * whose alias is x' even if there is a column called x.
     */
    public boolean isSortByAlias()
    {
        switch (this) {
        case Default:
        case Oracle10g:
        case Strict92:
            return true;
        default:
            return false;
        }
    }

    /**
     * Whether "empno" is invalid in "select empno as x from emp order by empno"
     * because the alias "x" obscures it.
     */
    public boolean isSortByAliasObscures()
    {
        return this == SqlConformance.Strict92;
    }
}

// End SqlConformance.java

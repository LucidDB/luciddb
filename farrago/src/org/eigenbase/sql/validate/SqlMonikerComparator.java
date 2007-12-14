/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2004-2007 The Eigenbase Project
// Copyright (C) 2004-2007 Disruptive Tech
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
package org.eigenbase.sql.validate;

import java.util.*;


/**
 * A general-purpose implementation of {@link Comparator} to compare
 * {@link SqlMoniker} values.
 *
 * @author tleung
 * @version $Id$
 * @since Oct 16, 2005
 */
public class SqlMonikerComparator
    implements Comparator<SqlMoniker>
{
    //~ Methods ----------------------------------------------------------------

    public int compare(SqlMoniker m1, SqlMoniker m2)
    {
        if (m1.getType().ordinal() > m2.getType().ordinal()) {
            return 1;
        } else if (m1.getType().ordinal() < m2.getType().ordinal()) {
            return -1;
        } else {
            return (m1.toString().compareTo(m2.toString()));
        }
    }
}

// End SqlMonikerComparator.java

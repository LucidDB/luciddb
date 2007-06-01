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

import java.util.*;


/**
 * A generic implementation of {@link SqlMonikerComparator} to compare {@link
 * SqlMoniker}.
 *
 * @author tleung
 * @version $$
 * @since Oct 16, 2005
 */
public class SqlMonikerComparator
    implements Comparator
{
    //~ Methods ----------------------------------------------------------------

    /*
     * Compares its arguments for order.  The arguments have to be of type
     * {@link SqlMoniker}
     */
    public int compare(Object o1, Object o2)
    {
        if (!(o1 instanceof SqlMoniker) || !(o2 instanceof SqlMoniker)) {
            return 0;
        }
        SqlMoniker m1 = (SqlMoniker) o1;
        SqlMoniker m2 = (SqlMoniker) o2;

        if (m1.getType().getOrdinal() > m2.getType().getOrdinal()) {
            return 1;
        } else if (m1.getType().getOrdinal() < m2.getType().getOrdinal()) {
            return -1;
        } else {
            return (m1.toString().compareTo(m2.toString()));
        }
    }
}

// End SqlMonikerComparator.java

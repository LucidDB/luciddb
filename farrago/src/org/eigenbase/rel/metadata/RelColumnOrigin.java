/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006-2007 The Eigenbase Project
// Copyright (C) 2006-2007 SQLstream, Inc.
// Copyright (C) 2006-2007 LucidEra, Inc.
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
package org.eigenbase.rel.metadata;

import java.util.*;

import org.eigenbase.relopt.*;


/**
 * RelColumnOrigin is a data structure describing one of the origins of an
 * output column produced by a relational expression.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class RelColumnOrigin
{
    //~ Instance fields --------------------------------------------------------

    private final RelOptTable originTable;

    private final int iOriginColumn;

    private final boolean isDerived;

    //~ Constructors -----------------------------------------------------------

    public RelColumnOrigin(
        RelOptTable originTable,
        int iOriginColumn,
        boolean isDerived)
    {
        this.originTable = originTable;
        this.iOriginColumn = iOriginColumn;
        this.isDerived = isDerived;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * @return table of origin
     */
    public RelOptTable getOriginTable()
    {
        return originTable;
    }

    /**
     * @return 0-based index of column in origin table; whether this ordinal is
     * flattened or unflattened depends on whether UDT flattening has already
     * been performed on the relational expression which produced this
     * description
     */
    public int getOriginColumnOrdinal()
    {
        return iOriginColumn;
    }

    /**
     * Consider the query <code>select a+b as c, d as e from t</code>. The
     * output column c has two origins (a and b), both of them derived. The
     * output column d as one origin (c), which is not derived.
     *
     * @return false if value taken directly from column in origin table; true
     * otherwise
     */
    public boolean isDerived()
    {
        return isDerived;
    }

    // override Object
    public boolean equals(Object obj)
    {
        if (!(obj instanceof RelColumnOrigin)) {
            return false;
        }
        RelColumnOrigin other = (RelColumnOrigin) obj;
        return Arrays.equals(
            originTable.getQualifiedName(),
            other.originTable.getQualifiedName())
            && (iOriginColumn == other.iOriginColumn)
            && (isDerived == other.isDerived);
    }

    // override Object
    public int hashCode()
    {
        return Arrays.hashCode(originTable.getQualifiedName())
            + iOriginColumn + (isDerived ? 313 : 0);
    }
}

// End RelColumnOrigin.java

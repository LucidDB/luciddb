/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006-2007 The Eigenbase Project
// Copyright (C) 2006-2007 Disruptive Tech
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

/**
 * RelColumnMapping records a mapping from an input column of a RelNode to one
 * of its output columns.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class RelColumnMapping
{
    //~ Instance fields --------------------------------------------------------

    /**
     * 0-based ordinal of mapped output column.
     */
    public int iOutputColumn;

    /**
     * 0-based ordinal of mapped input rel.
     */
    public int iInputRel;

    /**
     * 0-based ordinal of mapped column within input rel.
     */
    public int iInputColumn;

    /**
     * Whether the column mapping transforms the input.
     */
    public boolean isDerived;
}

// End RelColumnMapping.java

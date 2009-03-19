/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 SQLstream, Inc.
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
package org.eigenbase.stat;

/**
 * This interface provides results based on column statistics. It may be used to
 * summarize the results of applying a predicate to a column of a relational
 * expression. Alternatively, it may be used to summarize aspects of the entire
 * column.
 *
 * @author John Pham
 * @version $Id$
 */
public interface RelStatColumnStatistics
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Estimates the percentage of a relational expression's rows which satisfy
     * a given condition. This corresponds to the metadata query {@link
     * org.eigenbase.rel.metadata.RelMetadataQuery#getSelectivity}.
     *
     * @return an estimated percentage from 0.0 to 1.0 or null if no reliable
     * estimate can be determined
     */
    public Double getSelectivity();

    /**
     * Estimates the number of distinct values returned from a relational
     * expression that satisfy a given condition.
     *
     * @return an estimate of the distinct values of a predicate or null if no
     * reliable estimate can be determined
     */
    public Double getCardinality();

    /**
     * Determine how many blocks on disk will be read from physical storage
     * to retrieve the column values selected. This corresponds to an
     * attribute set by the Broadbase server. This feature is deferred until
     * we find a use for it
     */
    // public Long getNumBlocks();
}

// End RelStatColumnStatistics.java

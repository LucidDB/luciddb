/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
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
package org.eigenbase.stat;

import org.eigenbase.rel.*;
import org.eigenbase.sarg.*;


/**
 * This class encapsulates statistics for a RelNode
 *
 * @author John Pham
 * @version $Id$
 */
public interface RelStatSource
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Returns the number of rows in a relation, as determined by statistics
     *
     * @return a row count, or null if one could not be determined
     */
    Double getRowCount();

    /**
     * Returns statistics pertaining to a column specified by the 0-based
     * ordinal and the sargable predicates associated with that column. The
     * second argument can be null if there are no sargable predicates on the
     * column.
     *
     * @param ordinal zero based column ordinal
     * @param predicate associated predicates(s), evaluated as intervals
     *
     * @return filtered column statistics, or null if they could not be obtained
     */
    RelStatColumnStatistics getColumnStatistics(
        int ordinal,
        SargIntervalSequence predicate);
}

// End RelStatSource.java

/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006-2009 The Eigenbase Project
// Copyright (C) 2006-2009 SQLstream, Inc.
// Copyright (C) 2006-2009 LucidEra, Inc.
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
package org.eigenbase.runtime;

import java.sql.*;

import org.eigenbase.util.*;


public class TupleIterResultSet
    extends AbstractIterResultSet
{
    //~ Instance fields --------------------------------------------------------

    private final TupleIter tupleIter;
    private TimeoutQueueTupleIter timeoutTupleIter;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a result set based upon an iterator. The column-getter accesses
     * columns based upon their ordinal.
     *
     * @pre tupleIter != null
     */
    public TupleIterResultSet(
        TupleIter tupleIter,
        ColumnGetter columnGetter)
    {
        super(columnGetter);

        Util.pre(tupleIter != null, "tupleIter != null");
        this.tupleIter = tupleIter;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Sets the timeout that this TupleIterResultSet will wait for a row from
     * the underlying iterator.
     *
     * @param timeoutMillis Timeout in milliseconds. Must be greater than zero.
     */
    public void setTimeout(long timeoutMillis)
    {
        super.setTimeout(timeoutMillis);

        assert timeoutTupleIter == null;

        // we create a new semaphore for each executeQuery call
        // and then pass ownership to the result set returned
        // the query timeout used is the last set via JDBC.
        timeoutTupleIter = new TimeoutQueueTupleIter(tupleIter);
        timeoutTupleIter.start();
    }

    public void close()
        throws SQLException
    {
        if (timeoutTupleIter != null) {
            final long noTimeout = 0;
            timeoutTupleIter.closeAllocation(noTimeout);
            timeoutTupleIter = null;
        }
    }

    // ------------------------------------------------------------------------
    // the remaining methods implement ResultSet
    public boolean next()
        throws SQLException
    {
        if (maxRows > 0) {
            if (row >= maxRows) {
                return false;
            }
        }

        try {
            Object next =
                (timeoutTupleIter != null)
                ? timeoutTupleIter.fetchNext(timeoutMillis)
                : tupleIter.fetchNext();

            if (next == TupleIter.NoDataReason.END_OF_DATA) {
                return false;
            } else if (next instanceof TupleIter.NoDataReason) {
                // TODO: SWZ: 2/23/2006: better exception
                throw new RuntimeException();
            }

            current = next;
            row++;
            return true;
        } catch (QueueIterator.TimeoutException e) {
            throw new SqlTimeoutException();
        } catch (Throwable e) {
            throw newFetchError(e);
        }
    }
}

// End TupleIterResultSet.java

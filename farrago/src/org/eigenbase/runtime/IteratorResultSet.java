/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2002-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2003-2007 John V. Sichi
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

import java.util.*;

import org.eigenbase.util.*;


/**
 * A <code>IteratorResultSet</code> is an adapter which converts a {@link
 * java.util.Iterator} into a {@link java.sql.ResultSet}.
 *
 * <p>See also its converse adapter, {@link ResultSetIterator}</p>
 */
public class IteratorResultSet
    extends AbstractIterResultSet
{
    //~ Instance fields --------------------------------------------------------

    private final Iterator iterator;
    private TimeoutQueueIterator timeoutIter;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a result set based upon an iterator. The column-getter accesses
     * columns based upon their ordinal.
     *
     * @pre iterator != null
     */
    public IteratorResultSet(
        Iterator iterator,
        ColumnGetter columnGetter)
    {
        super(columnGetter);

        Util.pre(iterator != null, "iterator != null");
        this.iterator = iterator;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Sets the timeout that this IteratorResultSet will wait for a row from the
     * underlying iterator.
     *
     * @param timeoutMillis Timeout in milliseconds. Must be greater than zero.
     */
    public void setTimeout(long timeoutMillis)
    {
        super.setTimeout(timeoutMillis);

        assert timeoutIter == null;

        // we create a new semaphore for each executeQuery call
        // and then pass ownership to the result set returned
        // the query timeout used is the last set via JDBC.
        timeoutIter = new TimeoutQueueIterator(iterator);
        timeoutIter.start();
    }

    public void close()
        throws SQLException
    {
        if (timeoutIter != null) {
            final long noTimeout = 0;
            timeoutIter.close(noTimeout);
            timeoutIter = null;
        }
    }

    // ------------------------------------------------------------------------
    // the remaining methods implement ResultSet
    public boolean next()
        throws SQLException
    {
        if (timeoutIter != null) {
            try {
                long endTime = System.currentTimeMillis() + timeoutMillis;
                if (timeoutIter.hasNext(timeoutMillis)) {
                    long remainingTimeout =
                        endTime - System.currentTimeMillis();
                    if (remainingTimeout <= 0) {
                        // The call to hasNext() took longer than we
                        // expected -- we're out of time.
                        throw new SqlTimeoutException();
                    }
                    this.current = timeoutIter.next(remainingTimeout);
                    this.row++;
                    return true;
                } else {
                    return false;
                }
            } catch (QueueIterator.TimeoutException e) {
                throw new SqlTimeoutException();
            } catch (Throwable e) {
                throw newFetchError(e);
            }
        } else {
            try {
                if (iterator.hasNext()) {
                    this.current = iterator.next();
                    this.row++;
                    return true;
                } else {
                    return false;
                }
            } catch (Throwable e) {
                throw newFetchError(e);
            }
        }
    }
}

// End IteratorResultSet.java

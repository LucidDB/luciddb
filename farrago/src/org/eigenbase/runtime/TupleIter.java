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
package org.eigenbase.runtime;

import org.eigenbase.util.*;


/**
 * TupleIter provides an Iterator-like interface for reading tuple data.
 *
 * <p>TupleIter replaces the combination of {@link java.util.Iterator} and
 * {@link org.eigenbase.runtime.RestartableIterator}.
 *
 * <p>Note that calling {@link ClosableAllocation#closeAllocation()
 * closeAllocation()} closes this iterator, allowing it to release its
 * resources. No further calls to {@link TupleIter#fetchNext} or {@link
 * TupleIter#restart} may be made once the iterator is closed.
 *
 * @author Stephan Zuercher
 * @version $Id$
 */
public interface TupleIter
    extends ClosableAllocation
{
    //~ Static fields/initializers ---------------------------------------------

    public static final TupleIter EMPTY_ITERATOR =
        new TupleIter() {
            public Object fetchNext()
            {
                return NoDataReason.END_OF_DATA;
            }

            public boolean setTimeout(long timeout, boolean asUnderflow)
            {
                return false;
            }

            public void restart()
            {
            }

            public void closeAllocation()
            {
            }
        };

    //~ Enums ------------------------------------------------------------------

    /**
     * NoDataReason provides a reason why no data was returned by a call to
     * {@link TupleIter#fetchNext}.
     */
    public enum NoDataReason
    {
        /**
         * End of data. No more data will be returned unless the iterator is
         * reset by a call to {@link TupleIter#restart()}.
         */
        END_OF_DATA,

        /**
         * Data underflow. No more data will be returned until the underlying
         * data source provides more input rows.
         */
        UNDERFLOW,
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Returns the next element in the iteration. If there is no next value, it
     * returns a value from the {@link NoDataReason} enumeration indicating why
     * no data was returned.
     *
     * <p>If this method returns {@link NoDataReason#END_OF_DATA}, no further
     * data will be returned by this iterator unless {@link TupleIter#restart()}
     * is called.
     *
     * <p>If this method returns {@link NoDataReason#UNDERFLOW}, no data is
     * currently available, but may be come available in the future. It is
     * possible for consecutive calls to return UNDERFLOW and then END_OF_DATA.
     *
     * <p>The object returned by this method may be re-used for each subsequent
     * call to <code>fetchNext()</code>. In other words, callers must either
     * make certain that the returned value is no longer needed or is copied
     * before any subsequent calls to <code>fetchNext()</code>.
     *
     * @return the next element in the iteration, or an instance of {@link
     * NoDataReason}.
     */
    public Object fetchNext();

    // REVIEW mberkowitz 27-Nov-2008 Is this too contrived? Intended to support
    // FarragoTransform.execute() in data-push mode.
    /**
     * Sets a timeout for {@link TupleIter#fetchNext}; (optional operation). Not
     * all implementing classes support a timeout. For those that do, this
     * method provides a common interface, For those that do not, the adapter
     * {@link TimeoutQueueTupleIter} puts a timeout queue on top.
     *
     * @param timeout in milliseconds. 0 means poll, infinity means block.
     * @param asUnderflow true means indicate timeout by returning {@link
     * NoDataReason#UNDERFLOW}; false means throw {@link
     * TupleIter.TimeoutException} on a timeout.
     *
     * @return true if the timeout was set, false if the implementing class does
     * not support a timeout.
     */
    public boolean setTimeout(long timeout, boolean asUnderflow);

    /**
     * Restarts this iterator, so that a subsequent call to {@link
     * TupleIter#fetchNext()} returns the first element in the collection being
     * iterated.
     */
    public void restart();

    //~ Inner Classes ----------------------------------------------------------

    /**
     * One way to indicate that {@link TupleIter#fetchNext} timed-out. The other
     * way is to return {@link NoDataReason#UNDERFLOW}. See {@link
     * TupleIter#setTimeout}. Since throwing this exception is optional for
     * fetchNext, it is a RuntimeException.
     */
    public static class TimeoutException
        extends RuntimeException
    {
    }
}

// End TupleIter.java

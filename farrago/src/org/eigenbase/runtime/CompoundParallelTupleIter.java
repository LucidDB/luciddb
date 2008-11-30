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

import java.util.*;

import org.eigenbase.test.*;


/**
 * <code>CompoundParallelTupleIter</code> creates one TupleIter out of several.
 * Unlike its serial counterpart {@link CompoundTupleIter}, it runs all its
 * inputs in parallel, in separate threads that it spawns. It outputs the next
 * element available from any of its inputs. Note that the order of output rows
 * is indeterminate, since it is unpredictable which input will arrive next.
 *
 * <p>The compound TupleIter is finished when all of its inputs are finished.
 * The set of input iterators is fixed at construction.
 *
 * <p>This variant is needed when an input is infinite, since CompoundTupleIter
 * would hang. Extending this class to preserve order is problematic, given its
 * low level:
 *
 * <ul>
 * <li>items Are now synthetic {@link Object}s.</li>
 * <li>Items would have to become things that expose a {@link Comparable} <i>
 * key</i> value.</li>
 * <li>Even if one input lags behind the other provding a {@link
 * Iterator#next()} value, that missing value might sort before its available
 * counterparts from the other inputs. There is no basis to decide to wait for
 * it or not.</li>
 * </ul>
 *
 * @author Marc Berkowitz
 * @version $Id$
 */
public class CompoundParallelTupleIter
    extends AbstractTupleIter
{
    //~ Instance fields --------------------------------------------------------

    final private TupleIter [] in;
    final private boolean [] endOfData;
    private int i;

    //~ Constructors -----------------------------------------------------------

    public CompoundParallelTupleIter(TupleIter [] tupleIters)
    {
        this.in = tupleIters;
        this.endOfData = new boolean[tupleIters.length];
        this.i = -1;
    }

    //~ Methods ----------------------------------------------------------------

    public Object fetchNext()
    {
        int endOfDataCount = 0;

        final int N = in.length;

        for (int offset = 0; offset < N; offset++) {
            if (++i >= N) {
                i = 0;
            }

            if (endOfData[i]) {
                endOfDataCount++;
            } else {
                Object o = in[i].fetchNext();

                if (o == NoDataReason.END_OF_DATA) {
                    endOfData[i] = true;
                    endOfDataCount++;
                } else if (o == NoDataReason.UNDERFLOW) {
                    // Ignore this.
                } else {
                    return o;
                }
            }
        }

        if (endOfDataCount == N) {
            return NoDataReason.END_OF_DATA;
        }

        return NoDataReason.UNDERFLOW;
    }

    public void restart()
    {
        for (int index = 0; index < in.length; index++) {
            in[index].restart();
            endOfData[index] = false;
        }
        i = -1;
    }

    public void closeAllocation()
    {
        for (int index = 0; index < in.length; index++) {
            in[index].closeAllocation();
        }
    }

    //~ Inner Classes ----------------------------------------------------------

    public static class Test
        extends EigenbaseTestCase
    {
        public Test(String s)
            throws Exception
        {
            super(s);
        }

        // The CompoundParallelTupleIter preserves the order of 2 elements
        // from the same source, but may transpose 2 elements from different
        // soureces. Being sloppy, just test that the actual results match the
        // expected results when resorted.
        protected void assertEquals(
            TupleIter tupleIter,
            Object [] expected) // expected vals -- sorted in place

        {
            Object [] actual = toList(tupleIter).toArray(); // get results
            Arrays.sort(actual);
            Arrays.sort(expected);
            assertEquals(expected, actual);
        }

        public void testCompoundParallelTupleIter2()
        {
            TupleIter tupleIter =
                new CompoundParallelTupleIter(
                    new TupleIter[] {
                        makeTupleIter(new String[] { "a", "b" }),
                        makeTupleIter(new String[] { "c" })
                    });
            assertEquals(
                tupleIter,
                new String[] { "a", "b", "c" });
        }

        public void testCompoundParallelTupleIter1()
        {
            TupleIter tupleIter =
                new CompoundParallelTupleIter(
                    new TupleIter[] {
                        makeTupleIter(new String[] { "a", "b", "c" })
                    });
            assertEquals(
                tupleIter,
                new String[] { "a", "b", "c" });
        }

        public void testCompoundParallelTupleIter3()
        {
            TupleIter tupleIter =
                new CompoundParallelTupleIter(
                    new TupleIter[] {
                        makeTupleIter(new String[] { "a", "b", "c" }),
                        makeTupleIter(new String[] { "d", "e" }),
                        makeTupleIter(new String[] { "f" }),
                    });
            assertEquals(
                tupleIter,
                new String[] { "a", "b", "c", "d", "e", "f" });
        }

        public void testCompoundParallelIterEmpty1()
        {
            TupleIter tupleIter =
                new CompoundParallelTupleIter(new TupleIter[] {});
            assertEquals(
                tupleIter,
                new String[] {});
        }

        public void testCompoundParallelIterEmpty2()
        {
            TupleIter tupleIter =
                new CompoundParallelTupleIter(
                    new TupleIter[] {
                        makeTupleIter(new String[] {}),
                        makeTupleIter(new String[] { "a", "b" })
                    });
            assertEquals(
                tupleIter,
                new String[] { "a", "b" });
        }
    }
}

// End CompoundParallelTupleIter.java

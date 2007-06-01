/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2006 The Eigenbase Project
// Copyright (C) 2002-2006 Disruptive Tech
// Copyright (C) 2005-2006 LucidEra, Inc.
// Portions Copyright (C) 2003-2006 John V. Sichi
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
import java.util.logging.*;

import org.eigenbase.test.*;
import org.eigenbase.trace.*;


/**
 * <code>CompoundTupleIter</code> creates an iterator out of several iterators.
 *
 * <p>CompoundTupleIter is serial: it yields all the elements of its first input
 * Iterator, then all those of its second input, etc. When all inputs are
 * exhausted, it is done. (Cf {@link CompoundParallelTupleIter}.)
 *
 * @author Stephan Zuercher
 * @version $Id$
 */
public class CompoundTupleIter
    implements TupleIter
{
    //~ Static fields/initializers ---------------------------------------------

    private static final Logger tracer =
        EigenbaseTrace.getCompoundIteratorTracer();

    //~ Instance fields --------------------------------------------------------

    private TupleIter iterator;
    private TupleIter [] iterators;
    private int i;

    //~ Constructors -----------------------------------------------------------

    public CompoundTupleIter(TupleIter [] iterators)
    {
        this.iterators = iterators;
        this.i = 0;

        initIterator();
    }

    //~ Methods ----------------------------------------------------------------

    private void initIterator()
    {
        if (this.iterators.length > 0) {
            this.iterator = this.iterators[0];
        } else {
            this.iterator = TupleIter.EMPTY_ITERATOR;
        }
    }

    public Object fetchNext()
    {
        tracer.finer(toString());
        Object next = iterator.fetchNext();

        if (next == NoDataReason.END_OF_DATA) {
            i++;
            if (i < iterators.length) {
                iterator = iterators[i];
                tracer.fine("try " + iterator);
                return fetchNext();
            }

            return NoDataReason.END_OF_DATA;
        }

        return next;
    }

    public void restart()
    {
        // fetchNext() can be called repeatedly after it returns END_OF_DATA.
        // Even if it isn't, it uses recursion which implies an extra call
        // when END_OF_DATA on the last iterator is reached.
        // Each extra call increments i.  We want to restart all iterators
        // that we've touched (e.g. 0 to i, inclusive) but need to compensate
        // i that's grown too large.
        final int maxIndex = Math.min(i, iterators.length - 1);
        for (int index = 0; index <= maxIndex; index++) {
            iterators[index].restart();
        }

        i = 0;
        initIterator();
    }

    public void closeAllocation()
    {
        for (TupleIter iter : iterators) {
            iter.closeAllocation();
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

        public void testCompoundIter()
        {
            TupleIter iterator =
                new CompoundTupleIter(
                    new TupleIter[] {
                        makeTupleIter(new String[] { "a", "b" }),
                        makeTupleIter(new String[] { "c" })
                    });
            assertEquals(
                iterator,
                new String[] { "a", "b", "c" });
        }

        public void testCompoundIterEmpty()
        {
            TupleIter iterator = new CompoundTupleIter(new TupleIter[] {});
            assertEquals(
                iterator,
                new String[] {});
        }

        public void testCompoundIterFirstEmpty()
        {
            TupleIter iterator =
                new CompoundTupleIter(
                    new TupleIter[] {
                        makeTupleIter(new String[] {}),
                        makeTupleIter(new String[] { "a", null }),
                        makeTupleIter(new String[] {}),
                        makeTupleIter(new String[] {}),
                        makeTupleIter(new String[] { "b", "c" }),
                        makeTupleIter(new String[] {})
                    });
            assertEquals(
                iterator,
                new String[] { "a", null, "b", "c" });
        }

        // makes a trivial CalcTupleIter on top of a base TupleIter
        private static CalcTupleIter makeCalcTupleIter(final TupleIter base)
        {
            return new CalcTupleIter(base) {
                public Object fetchNext()
                {
                    return base.fetchNext();
                }
            };
        }

        public void testCompoundCalcIter()
        {
            TupleIter iterator =
                new CompoundTupleIter(
                    new TupleIter[] {
                        makeCalcTupleIter(
                            makeTupleIter(new String[] { "a", "b" })),
                        makeCalcTupleIter(makeTupleIter(new String[] { "c" }))
                    });
            assertEquals(
                iterator,
                new String[] { "a", "b", "c" });
        }

        public void testCompoundCalcIterFirstEmpty()
        {
            TupleIter iterator =
                new CompoundTupleIter(
                    new TupleIter[] {
                        makeCalcTupleIter(makeTupleIter(new String[] {})),
                        makeCalcTupleIter(makeTupleIter(new String[] { "a" })),
                        makeCalcTupleIter(makeTupleIter(new String[] {})),
                        makeCalcTupleIter(makeTupleIter(new String[] {})),
                        makeCalcTupleIter(
                            makeTupleIter(new String[] { "b", "c" })),
                        makeCalcTupleIter(makeTupleIter(new String[] {}))
                    });
            assertEquals(
                iterator,
                new String[] { "a", "b", "c" });
        }

        /**
         * Checks that a BoxTupleIter returns the same values as the contents of
         * an array.
         */
        protected void assertUnboxedEquals(TupleIter p, Object [] a)
        {
            ArrayList<Object> list = new ArrayList<Object>();
            while (true) {
                Object o = p.fetchNext();
                if (o instanceof Box) {
                    list.add(((Box) o).getValue());
                } else if (o == NoDataReason.END_OF_DATA) {
                    break;
                } else {
                    list.add(o);
                }
            }
            assertEquals(list, a);
        }

        public void testCompoundBoxIter()
        {
            TupleIter iterator =
                new CompoundTupleIter(
                    new TupleIter[] {
                        new BoxTupleIter(
                            makeTupleIter(
                                new String[] { "400", "401", "402", "403" })),
                        new BoxTupleIter(
                            makeTupleIter(
                                new String[] { "500", "501", "502", "503" })),
                        new BoxTupleIter(
                            makeTupleIter(
                                new String[] { "600", "601", "602", "603" }))
                    });
            assertUnboxedEquals(
                iterator,
                new String[] {
                    "400", "401", "402", "403",
                    "500", "501", "502", "503",
                    "600", "601", "602", "603"
                });
        }

        public void testCompoundBoxedCalcIter()
        {
            TupleIter iterator =
                new CompoundTupleIter(
                    new TupleIter[] {
                        new BoxTupleIter(
                            makeCalcTupleIter(
                                makeTupleIter(
                                    new String[] { "400", "401", "402", "403" }))),
                        new BoxTupleIter(
                            makeCalcTupleIter(
                                makeTupleIter(
                                    new String[] { "500", "501", "502", "503" }))),
                        new BoxTupleIter(
                            makeCalcTupleIter(
                                makeTupleIter(
                                    new String[] { "600", "601", "602", "603" })))
                    });
            assertUnboxedEquals(
                iterator,
                new String[] {
                    "400", "401", "402", "403",
                    "500", "501", "502", "503",
                    "600", "601", "602", "603"
                });
        }

        // a boxed value (see BoxTupleIter below)
        static class Box
        {
            Object val;

            public Box()
            {
                val = null;
            }

            public Object getValue()
            {
                return val;
            }

            public Box setValue(Object val)
            {
                this.val = val;
                return this;
            }
        }

        // An TupleIter that always returns the same object, a Box, but with
        // different contents. Mimics the TupleIter from a farrago dynamic
        // statement.
        static class BoxTupleIter
            implements TupleIter
        {
            TupleIter base;
            Box box;

            public BoxTupleIter(TupleIter base)
            {
                this.base = base;
                this.box = new Box();
            }

            // implement TupleIter
            public Object fetchNext()
            {
                Object result = base.fetchNext();
                if (result instanceof NoDataReason) {
                    return result;
                }

                box.setValue(result);
                return box;
            }

            // implement TupleIter
            public void restart()
            {
                throw new UnsupportedOperationException();
            }

            // implement TupleIter
            public void closeAllocation()
            {
                box = null;
            }
        }
    }
}

// End CompoundTupleIter.java

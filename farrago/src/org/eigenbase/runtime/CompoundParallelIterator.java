/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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

import java.util.Iterator;
import java.util.Arrays;
import java.util.logging.Logger;
import org.eigenbase.test.EigenbaseTestCase;
import org.eigenbase.trace.EigenbaseTrace;

/**
 * <code>CompoundParallelIterator</code> creates one iterator out of several.
 * Unlike its serial counterpart {@link CompoundIterator}, it runs all its inputs in
 * parallel, in separate threads that it spawns. It outputs the next element available
 * from any of its inputs. Note that the order of output rows is indeterminate, since it is
 * unpredictable which input will arrive next.
 *<p>
 * The compound iterator is finished when all of its inputs are finished. The set of
 * input iterators is fixed at construction.
 * <p>
 * This variant is needed when an input is infinite, since CompoundIterator would hang.
 * Extending this class to preserve order is problematic, given its low level:<ul>
 * <li>items Are now synthetic <code>Object</code>s.</li>
 * <li>Items would have to become things that expose a <code>Comparable</code> <i>key</i> value.</li>
 * <li>Even if one input lags behind the other provding a <code>next()</code> value, that missing
 *     value might sort before its available counterparts from the other inputs.
 *     There is no basis to decide to wait for it or not.</li>
 *</ul>
 *
 */
public class CompoundParallelIterator implements Iterator
{
    private static final Logger tracer = EigenbaseTrace.getCompoundParallelIteratorTracer();

    //~ Instance fields -------------------------------------------------------
    final private Iterator [] in;
    private QueueIterator out;

    //~ Constructors ----------------------------------------------------------

    public CompoundParallelIterator(Iterator [] iterators)
    {
        this.in = iterators;
        this.out = new QueueIterator(in.length, tracer);
        for (int i = 0; i < in.length; i++) {
            Thread th = new InputIteratorThread(out, in[i]);
            th.start();
        }
    }

    // a subthread that feeds from one input Iterator into the common output
    private static class InputIteratorThread extends Thread {
        final private Iterator in;
        final private QueueIterator out;
        public InputIteratorThread(QueueIterator out, Iterator in) {
            this.out = out;
            this.in = in;
        }
        public void run() {
            while (in.hasNext()) {
                Object o = in.next();
                out.put(o);
            }
            out.done(null);
        }
    }

    //~ Methods ---------------------------------------------------------------

    public boolean hasNext()
    {
        return out.hasNext();
    }

    public Object next()
    {
        return out.next();
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    public void restart()
    {
        throw new UnsupportedOperationException();
    }

    //~ inner test class
    public static class Test extends EigenbaseTestCase
    {
        public Test(String s) throws Exception 
        {
            super(s);
        }

        // The CompoundParallelIterator preserves the order of 2 elements
        // from the same source, but may transpose 2 elements from different
        // soureces. Being sloppy, just test that the actual results match the
        // expected results when resorted.
        protected void assertEquals(
            Iterator iterator,
            Object[] expected)          // expected vals -- sorted in place
        {
            Object actual[] = toList(iterator).toArray(); // get results
            Arrays.sort(actual);
            Arrays.sort(expected);
            assertEquals(expected, actual);
        }

        public void testCompoundParallelIter2()
        {
            Iterator iterator =
                new CompoundParallelIterator(new Iterator [] {
                    makeIterator(new String [] { "a", "b"}),
                    makeIterator(new String [] { "c" })
                });
            assertEquals(
                iterator,
                new String [] { "a", "b", "c" });
        }

        public void testCompoundParallelIter1()
        {
            Iterator iterator =
                new CompoundParallelIterator(new Iterator [] {
                    makeIterator(new String [] { "a", "b", "c"})
                });
            assertEquals(
                iterator,
                new String [] { "a", "b", "c" });
        }

        public void testCompoundParallelIter3()
        {
            Iterator iterator =
                new CompoundParallelIterator(new Iterator [] {
                    makeIterator(new String [] { "a", "b", "c"}),
                    makeIterator(new String [] { "d", "e"}),
                    makeIterator(new String [] { "f"}),
                });
            assertEquals(
                iterator,
                new String [] { "a", "b", "c", "d", "e", "f"  });
        }

        public void testCompoundParallelIterEmpty1()
        {
            Iterator iterator = new CompoundParallelIterator(new Iterator [] {  });
            assertEquals(
                iterator,
                new String [] {  });
        }

        public void testCompoundParallelIterEmpty2()
        {
            Iterator iterator =
                new CompoundParallelIterator(new Iterator [] {
                    makeIterator(new String [] { } ),
                    makeIterator(new String [] { "a", "b" })
                });
            assertEquals(
                iterator,
                new String [] { "a", "b" });
        }
    }
} 


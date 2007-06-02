/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
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
package org.eigenbase.test;

import java.util.*;
import java.util.concurrent.*;

import org.eigenbase.runtime.*;
import org.eigenbase.util.*;


/**
 * Test for {@link ThreadIterator}.
 *
 * @author Julian Hyde
 * @version $Id$
 */
public class ThreadIteratorTest
    extends EigenbaseTestCase
{
    //~ Constructors -----------------------------------------------------------

    public ThreadIteratorTest(String s)
        throws Exception
    {
        super(s);
    }

    //~ Methods ----------------------------------------------------------------

    public void testBeatlesSynchronous()
    {
        testBeatles(null);
    }

    public void testBeatlesPipelined()
    {
        testBeatles(new ArrayBlockingQueue(2));
    }

    private void testBeatles(BlockingQueue queue)
    {
        Iterator beatles =
            new ThreadIterator(queue) {
                String [] strings;

                public ThreadIterator start(String [] strings)
                {
                    this.strings = strings;
                    return start();
                }

                protected void doWork()
                {
                    for (int i = 0; i < strings.length; i++) {
                        put(new Integer(strings[i].length()));
                    }
                }
            }.start(new String[] { "lennon", "mccartney", null, "starr" });
        assertTrue(beatles.hasNext());
        assertEquals(
            beatles.next(),
            new Integer(6));
        assertEquals(
            beatles.next(),
            new Integer(9));
        boolean barf = false;
        try {
            Util.discard(beatles.next());
        } catch (NullPointerException e) {
            barf = true;
        }
        assertTrue("expected a NullPointerException", barf);
    }

    public void testDigits()
    {
        Iterator digits =
            new ThreadIterator() {
                int limit;

                public ThreadIterator start(int limit)
                {
                    this.limit = limit;
                    return super.start();
                }

                protected void doWork()
                {
                    for (int i = 0; i < limit; i++) {
                        put(new Integer(i));
                    }
                }
            }.start(10);
        assertEquals(
            digits,
            new Integer[] {
                new Integer(0), new Integer(1), new Integer(2),
                new Integer(3), new Integer(4), new Integer(5),
                new Integer(6), new Integer(7), new Integer(8),
                new Integer(9)
            });
        assertTrue(!digits.hasNext());
    }

    public void testEmpty()
    {
        Object [] empty = new Object[0];
        assertEquals(new ArrayIterator(empty),
            empty);
    }

    public void testXyz()
    {
        String [] xyz = new String[] { "x", "y", "z" };
        assertEquals(new ArrayIterator(xyz),
            xyz);
    }

    //~ Inner Classes ----------------------------------------------------------

    private static class ArrayIterator
        extends ThreadIterator
    {
        Object [] a;

        ArrayIterator(Object [] a)
        {
            this.a = a;
            start();
        }

        protected void doWork()
        {
            for (int i = 0; i < a.length; i++) {
                this.put(a[i]);
            }
        }
    }
}
// End ThreadIteratorTest.java

/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
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

import java.util.*;

import org.eigenbase.test.EigenbaseTestCase;


/**
 * <code>BufferedIterator</code> converts a regular iterator into one which
 * implements {@link Iterable} (and {@link Enumeration} for good measure).
 *
 * <p>
 * <i>Implementation note</i>: The first time you read from it, it duplicates
 * objects into a list. The next time, it creates an iterator from that list.
 * The implementation handles infinite iterators gracefully: it copies
 * objects onto the replay list only when they are requested for the first
 * time.
 * </p>
 *
 * @author jhyde
 * @version $Id$
 *
 * @since 26 April, 2002
 */
public class BufferedIterator implements Iterator, Iterable, Enumeration
{
    //~ Instance fields -------------------------------------------------------

    private Clonerator clonerator;
    private Iterator iterator;
    private List list;

    //~ Constructors ----------------------------------------------------------

    public BufferedIterator(Iterator iterator)
    {
        this.list = new ArrayList();
        this.clonerator = new Clonerator(iterator, list);
        this.iterator = clonerator;
    }

    //~ Methods ---------------------------------------------------------------

    // implement Enumeration
    public boolean hasMoreElements()
    {
        return iterator.hasNext();
    }

    // implement Iterator
    public boolean hasNext()
    {
        return iterator.hasNext();
    }

    // implement Iterable
    public Iterator iterator()
    {
        restart();
        return this;
    }

    // implement Iterator
    public Object next()
    {
        return iterator.next();
    }

    // implement Enumeration
    public Object nextElement()
    {
        return iterator.next();
    }

    // implement Iterator
    public void remove()
    {
        iterator.remove();
    }

    // implement Restartable
    public void restart()
    {
        if (clonerator == null) {
            // We have already read everything from the clonerator and
            // discarded it.
            iterator = list.iterator();
        } else if (!clonerator.hasNext()) {
            // They read everything from the clonerator. We can discard it
            // now.
            clonerator = null;
            iterator = list.iterator();
        } else {
            // Still stuff left in the clonerator. Create a compound
            // iterator, so that if they go further next time, it will
            // read later stuff from the clonerator.
            iterator =
                new CompoundIterator(
                    new Iterator [] { list.iterator(), clonerator });
        }
    }

    //~ Inner Classes ---------------------------------------------------------

    public static class Test extends EigenbaseTestCase
    {
        public Test(String s)
            throws Exception
        {
            super(s);
        }

        // --------------------------------------------------------------------
        // test BufferedIterator
        public void testBufferedIterator()
        {
            String [] abc = new String [] { "a", "b", "c" };
            Iterator source = makeIterator(abc);
            BufferedIterator iterator = new BufferedIterator(source);
            assertTrue(iterator.hasNext());
            assertTrue(iterator.next().equals("a"));

            // no intervening "hasNext"
            assertTrue(iterator.next().equals("b"));

            // restart before we get to the end
            iterator.restart();
            assertTrue(iterator.hasNext());
            assertEquals(iterator, abc);
            assertTrue(!iterator.hasNext());
            assertTrue(!iterator.hasNext());
            iterator.restart();
            assertEquals(iterator, abc);
        }

        // --------------------------------------------------------------------
        // test Clonerator
        public void testClonerator()
        {
            String [] ab = new String [] { "a", "b" };
            Iterator source = makeIterator(ab);
            List list = new ArrayList();
            Clonerator clonerator = new Clonerator(source, list);
            assertEquals(clonerator, ab);
            assertEquals(list, ab);
        }

        // --------------------------------------------------------------------
        // test CompoundIterator
        public void testCompoundIter()
        {
            Iterator iterator =
                new CompoundIterator(new Iterator [] {
                        makeIterator(new String [] { "a", "b" }),
                        makeIterator(new String [] { "c" })
                    });
            assertEquals(
                iterator,
                new String [] { "a", "b", "c" });
        }

        public void testCompoundIterEmpty()
        {
            Iterator iterator = new CompoundIterator(new Iterator [] {  });
            assertEquals(
                iterator,
                new String [] {  });
        }

        public void testCompoundIterFirstEmpty()
        {
            Iterator iterator =
                new CompoundIterator(new Iterator [] {
                        makeIterator(new String [] {  }),
                        makeIterator(new String [] { "a", null }),
                        makeIterator(new String [] {  }),
                        makeIterator(new String [] {  }),
                        makeIterator(new String [] { "b", "c" }),
                        makeIterator(new String [] {  })
                    });
            assertEquals(
                iterator,
                new String [] { "a", null, "b", "c" });
        }
    }

    /**
     * Reads from an iterator, duplicating elements into a list as it does so.
     */
    private static class Clonerator implements Iterator
    {
        Iterator iterator;
        List list;

        Clonerator(
            Iterator iterator,
            List list)
        {
            this.iterator = iterator;
            this.list = list;
        }

        public boolean hasNext()
        {
            return iterator.hasNext();
        }

        public Object next()
        {
            Object o = iterator.next();
            list.add(o);
            return o;
        }

        public void remove()
        {
            iterator.remove();
        }
    }
}


// End BufferedIterator.java

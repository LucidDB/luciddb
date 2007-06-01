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

import java.util.*;
import java.util.logging.*;

import org.eigenbase.test.*;
import org.eigenbase.trace.*;
import org.eigenbase.util.*;


/**
 * <code>CompoundIterator</code> creates an iterator out of several.
 * CompoundIterator is serial: it yields all the elements of its first input
 * Iterator, then all those of its second input, etc. When all inputs are
 * exhausted, it is done.
 *
 * <p>NOTE jvs 21-Mar-2006: This class is no longer used except by Saffron, but
 * is generally useful. Should probably be moved to a utility package.
 */
public class CompoundIterator
    implements RestartableIterator
{
    //~ Static fields/initializers ---------------------------------------------

    private static final Logger tracer =
        EigenbaseTrace.getCompoundIteratorTracer();

    //~ Instance fields --------------------------------------------------------

    private Iterator iterator;
    private Iterator [] iterators;
    private int i;

    //~ Constructors -----------------------------------------------------------

    public CompoundIterator(Iterator [] iterators)
    {
        this.iterators = iterators;
        this.i = 0;
    }

    //~ Methods ----------------------------------------------------------------

    public boolean hasNext()
    {
        tracer.finer(toString());
        if (iterator == null) {
            return nextIterator();
        }
        if (iterator.hasNext()) {
            return true;
        }
        return nextIterator();
    }

    public Object next()
    {
        tracer.finer(toString());
        if (iterator == null) {
            nextIterator();
        }
        return iterator.next();
    }

    public void remove()
    {
        tracer.finer(toString());
        if (iterator == null) {
            nextIterator();
        }
        iterator.remove();
    }

    // moves to the next child iterator, skipping any empty ones, and returns
    // true. when all the child iteratators are used up, return false;
    private boolean nextIterator()
    {
        while (i < iterators.length) {
            iterator = iterators[i++];
            tracer.fine("try " + iterator);
            if (iterator.hasNext()) {
                return true;
            }
        }
        tracer.fine("exhausted iterators");
        iterator = Collections.EMPTY_LIST.iterator();
        return false;
    }

    // implement RestartableIterator
    public void restart()
    {
        for (int j = 0; j < i; ++j) {
            Util.restartIterator(iterators[j]);
        }
        i = 0;
        iterator = null;
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
            Iterator iterator =
                new CompoundIterator(
                    new Iterator[] {
                        makeIterator(new String[] { "a", "b" }),
                        makeIterator(new String[] { "c" })
                    });
            assertEquals(
                iterator,
                new String[] { "a", "b", "c" });
        }

        public void testCompoundIterEmpty()
        {
            Iterator iterator = new CompoundIterator(new Iterator[] {});
            assertEquals(
                iterator,
                new String[] {});
        }

        public void testCompoundIterFirstEmpty()
        {
            Iterator iterator =
                new CompoundIterator(
                    new Iterator[] {
                        makeIterator(new String[] {}),
                        makeIterator(new String[] { "a", null }),
                        makeIterator(new String[] {}),
                        makeIterator(new String[] {}),
                        makeIterator(new String[] { "b", "c" }),
                        makeIterator(new String[] {})
                    });
            assertEquals(
                iterator,
                new String[] { "a", null, "b", "c" });
        }

        /**
         * Checks that a BoxIterator returns the same values as the contents of
         * an array.
         */
        protected void assertUnboxedEquals(Iterator p, Object [] a)
        {
            ArrayList list = new ArrayList();
            while (p.hasNext()) {
                Object o = p.next();
                if (o instanceof Box) {
                    list.add(((Box) o).getValue());
                } else {
                    list.add(o);
                }
            }
            assertEquals(list, a);
        }

        public void testCompoundBoxIter()
        {
            Iterator iterator =
                new CompoundIterator(
                    new Iterator[] {
                        new BoxIterator(
                            makeIterator(
                                new String[] { "400", "401", "402", "403" })),
                        new BoxIterator(
                            makeIterator(
                                new String[] { "500", "501", "502", "503" })),
                        new BoxIterator(
                            makeIterator(
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

        // a boxed value (see BoxIterator below)
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

        // An Iterator that always returns the same object, a Box, but with
        // different contents. Mimics the Iterator from a farrago dynamic
        // statement.
        static class BoxIterator
            implements Iterator
        {
            Iterator base;
            Box box;

            public BoxIterator(Iterator base)
            {
                this.base = base;
                this.box = new Box();
            }

            // implement Iterator
            public boolean hasNext()
            {
                return base.hasNext();
            }

            public Object next()
            {
                box.setValue(base.next());
                return box;
            }

            public void remove()
            {
                base.remove();
            }
        }
    }
}
// End CompoundIterator.java

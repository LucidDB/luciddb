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

package org.eigenbase.util;

import java.util.Comparator;
import java.util.NoSuchElementException;

import junit.framework.TestCase;


/**
 * A <code>BinaryHeap</code> is a heap implementation of a priority queue. It
 * is similar to BinaryHeap in apache commons, but it also has a {@link
 * #remove} method.
 *
 * @author jhyde
 * @version $Id $
 *
 * @since Dec 8, 2002
 */
public class BinaryHeap <T>
{
    //~ Instance fields -------------------------------------------------------

    private final Comparator<T> comparator;
    private T [] elements;
    private int count;

    //~ Constructors ----------------------------------------------------------

    public BinaryHeap(
        boolean isMin,
        Comparator<T> comparator)
    {
        if (isMin) {
            this.comparator = comparator;
        } else {
            final Comparator comparator1 = comparator;
            this.comparator =
                new Comparator<T>() {
                        public int compare(
                            T o1,
                            T o2)
                        {
                            return comparator1.compare(o2, o1);
                        }
                    };
        }
        this.elements = (T[]) new Object[7];
    }

    //~ Methods ---------------------------------------------------------------

    public boolean isEmpty()
    {
        return count == 0;
    }

    public void clear()
    {
        count = 0;
    }

    public void insert(T o)
    {
        if (++count >= elements.length) {
            expand();
        }
        elements[count] = o;
        percolateUp(count);
    }

    public T peek()
        throws NoSuchElementException
    {
        if (count < 1) {
            throw new NoSuchElementException();
        } else {
            return elements[1];
        }
    }

    public T pop()
        throws NoSuchElementException
    {
        T o = peek();
        elements[1] = elements[count--];
        percolateDown(1);
        return o;
    }

    /**
     * Removes an element from the queue. If it exists several times, removes
     * only one occurrence.
     *
     * @param o the object to remove
     *
     * @return whether the object was on the queue
     */
    public boolean remove(T o)
    {
        int j = find(o);
        if (j > 0) {
            elements[j] = elements[count--];
            percolateDown(j);
            return true;
        } else {
            return false;
        }
    }

    private void expand()
    {
        int newSize = (elements.length * 2) + 3;
        T [] oldElements = elements;
        elements = (T[]) new Object[newSize];
        System.arraycopy(oldElements, 1, elements, 1, oldElements.length - 1);
    }

    /**
     * Returns the first position of an element on the heap, or 0 if not found.
     */
    private int find(T o)
    {
        for (int i = 1; i <= count; i++) {
            T element = elements[i];
            if (element.equals(o)) {
                return i;
            }
        }
        return 0;
    }

    // Can still optimize:
    // 1. don't write 'o' each time, only when we're done
    // 2. re-order the if .. else if .. else to optimize the common case
    private void percolateDown(int i)
    {
        while (true) {
            T o = elements[i];
            int i2 = i * 2;
            if (i2 > count) {
                return;
            } else if ((i2 + 1) > count) {
                final T o2 = elements[i2];
                if (comparator.compare(o, o2) > 0) {
                    elements[i] = o2;
                    elements[i2] = o;
                }
                return;
            } else {
                T o2 = elements[i2];
                final T o3 = elements[i2 + 1];

                // compare with the smaller of [i2], [i2+1]
                if (comparator.compare(o2, o3) > 0) {
                    i2++;
                    o2 = o3;
                }
                if (comparator.compare(o, o2) > 0) {
                    elements[i] = o2;
                    elements[i2] = o;
                    i = i2;
                } else {
                    // [i] is not greater than its smaller child, the heap
                    // property is satisfied
                    return;
                }
            }
        }
    }

    private void percolateUp(int i)
    {
        int i2;
        while ((i2 = i / 2) > 0) {
            T o = elements[i];
            T o2 = elements[i2];
            if (comparator.compare(o, o2) >= 0) {
                // 'o' is greater than or equal to its parent, so the heap
                // property is satisfied, and we can stop
                return;
            } else {
                // 'o' is still less than its parent, swap with its parent and
                // continue
                elements[i] = o2;
                elements[i2] = o;
                i = i2;
            }
        }
    }

    //~ Inner Classes ---------------------------------------------------------

    public static class BinaryHeapTestCase extends TestCase
    {
        public BinaryHeapTestCase(String name)
        {
            super(name);
        }

        public void test()
        {
            final String [] a =
            { "3", "1", "4", "1", "5", "9", "2", "6", "5", "3", "5" };
            final BinaryHeap<String> heap =
                new BinaryHeap<String>(
                    true,
                    new Comparator<String>() {
                        public int compare(
                            String o1,
                            String o2)
                        {
                            return ((Comparable) o1).compareTo(o2);
                        }
                    });
            validate(heap);
            for (int i = 0; i < a.length; i++) {
                heap.insert(a[i]);
                validate(heap);
            }
            assertTrue(heap.remove("5"));
            validate(heap);
            assertTrue(heap.remove("1"));
            validate(heap);
            assertTrue(!heap.remove("0"));
            validate(heap);
            StringBuffer buf = new StringBuffer();
            while (!heap.isEmpty()) {
                validate(heap);
                final String s = heap.pop();
                validate(heap);
                buf.append(s);
            }
            validate(heap);
            final String actual = buf.toString();
            assertEquals("123345569", actual);
        }

        public void testMax()
        {
            BinaryHeap<String> heap =
                new BinaryHeap<String>(
                    false,
                    new Comparator<String>() {
                        public int compare(
                            String o1,
                            String o2)
                        {
                            return o1.compareTo(o2);
                        }
                    });
            heap.insert("parsley");
            heap.insert("sage");
            heap.insert("rosemary");
            heap.insert("thyme");
            assertEquals(
                "thyme",
                heap.pop());
            assertEquals(
                "sage",
                heap.pop());
            assertEquals(
                "rosemary",
                heap.pop());
            assertEquals(
                "parsley",
                heap.pop());
            assertTrue(heap.isEmpty());
        }

        void validate(BinaryHeap heap)
        {
            assertTrue(heap.count <= heap.elements.length);
            for (int i = 1; i <= heap.count; i++) {
                Object element = heap.elements[i];
                assertNotNull(element);
                int parent = i / 2;
                if (parent > 0) {
                    final int c =
                        heap.comparator.compare(heap.elements[parent], element);
                    assertTrue(c <= 0);
                }
            }
        }
    }
}


// End BinaryHeap.java

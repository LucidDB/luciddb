/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2002 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 2003 John V. Sichi
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

import java.util.*;

/**
 * Pair of objects.
 *
 * @author jhyde
 * @version $Id$
 * @since Oct 17, 2007
 */
public class Pair<T1, T2> implements Map.Entry<T1, T2>
{
    //~ Instance fields --------------------------------------------------------

    public final T1 left;
    public final T2 right;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a Pair.
     *
     * @param left left value
     * @param right right value
     */
    public Pair(T1 left, T2 right)
    {
        this.left = left;
        this.right = right;
    }

    /**
     * Creates a Pair of appropriate type.
     *
     * <p>This is a shorthand that allows you to omit implicit types. For
     * example, you can write:
     * <blockquote>return Pair.of(s, n);</blockquote>
     * instead of
     * <blockquote>return new Pair&lt;String, Integer&gt;(s, n);</blockquote>
     *
     * @param left left value
     * @param right right value
     * @return A Pair
     */
    public static <T1, T2> Pair<T1, T2> of(T1 left, T2 right)
    {
        return new Pair<T1, T2>(left, right);
    }

    //~ Methods ----------------------------------------------------------------

    public String toString()
    {
        return "<" + left + ", " + right + ">";
    }

    public boolean equals(Object obj)
    {
        return (obj instanceof Pair)
            && Util.equal(this.left, ((Pair) obj).left)
            && Util.equal(this.right, ((Pair) obj).right);
    }

    public int hashCode()
    {
        int h1 = Util.hash(0, left);
        return Util.hash(h1, right);
    }

    public T1 getKey()
    {
        return left;
    }

    public T2 getValue()
    {
        return right;
    }

    public T2 setValue(T2 value)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Converts a collection of Pairs into a Map.
     *
     * <p>This is an obvious thing to do because Pair is similar in structure to
     * {@link java.util.Map.Entry}.
     *
     * <p>The map contains a copy of the collection of Pairs; if you change the
     * collection, the map does not change.
     *
     * @param pairs Collection of Pair objects
     *
     * @return map with the same contents as the collection
     */
    public static <K, V> Map<K, V> toMap(Collection<Pair<K, V>> pairs)
    {
        HashMap<K, V> map = new HashMap<K, V>();
        for (Pair<K, V> pair : pairs) {
            map.put(pair.left, pair.right);
        }
        return map;
    }

    /**
     * Given a list of pairs, returns a list of the left elements of each pair.
     *
     * @param pairList List of pairs
     * @return List of left elements
     */
    public static <K, V> List<K> projectLeft(final List<Pair<K, V>> pairList)
    {
        return new AbstractList<K>()
        {
            public K get(int index)
            {
                return pairList.get(index).left;
            }

            public int size()
            {
                return pairList.size();
            }
        };
    }

    /**
     * Given a list of pairs, returns a list of the right elements of each pair.
     *
     * @param pairList List of pairs
     * @return List of right elements
     */
    public static <K, V> List<V> projectRight(final List<Pair<K, V>> pairList)
    {
        return new AbstractList<V>()
        {
            public V get(int index)
            {
                return pairList.get(index).right;
            }

            public int size()
            {
                return pairList.size();
            }
        };
    }

    /**
     * Given a list of entries, returns a list of the left elements of each
     * entry.
     *
     * <p>(Similar to {@link #projectLeft}, just a little less efficient.)</p>
     *
     * @param entryList List of entries
     * @return List of left elements
     */
    public static <K, V> List<K> projectKeys(
        final List<? extends Map.Entry<K, V>> entryList)
    {
        return new AbstractList<K>()
        {
            public K get(int index)
            {
                return entryList.get(index).getKey();
            }

            public int size()
            {
                return entryList.size();
            }
        };
    }

    /**
     * Given a list of entries, returns a list of the right elements of each
     * entry.
     *
     * <p>(Similar to {@link #projectLeft}, just a little less efficient.)</p>
     *
     * @param entryList List of entries
     * @return List of right elements
     */
    public static <K, V> List<V> projectValues(
        final List<? extends Map.Entry<K, V>> entryList)
    {
        return new AbstractList<V>()
        {
            public V get(int index)
            {
                return entryList.get(index).getValue();
            }

            public int size()
            {
                return entryList.size();
            }
        };
    }

    /**
     * Creates an iterable that iterates in parallel over a pair of iterables.
     *
     * @param i0 First iterable
     * @param i1 Second iterable
     * @param <K> Key type (element type of first iterable)
     * @param <V> Value type (element type of second iterable)
     * @return Iterable in over both iterables in parallel
     */
    public static <K, V> java.lang.Iterable<Pair<K, V>> of(
        final Iterable<K> i0,
        final Iterable<V> i1)
    {
        return new Iterable<Pair<K, V>>()
        {
            public Iterator<Pair<K, V>> iterator()
            {
                final Iterator<K> iterator0 = i0.iterator();
                final Iterator<V> iterator1 = i1.iterator();

                return new Iterator<Pair<K, V>>()
                {
                    public boolean hasNext()
                    {
                        final boolean hasNext0 = iterator0.hasNext();
                        final boolean hasNext1 = iterator1.hasNext();
                        assert hasNext0 == hasNext1;
                        return hasNext0 && hasNext1;
                    }

                    public Pair<K, V> next()
                    {
                        return Pair.of(iterator0.next(), iterator1.next());
                    }

                    public void remove()
                    {
                        iterator0.remove();
                        iterator1.remove();
                    }
                };
            }
        };
    }
}

// End Pair.java

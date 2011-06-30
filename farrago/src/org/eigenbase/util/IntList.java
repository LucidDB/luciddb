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
 * Extension to {@link ArrayList} to help build an array of <code>int</code>
 * values.
 *
 * @author jhyde
 * @version $Id$
 */
public class IntList
    extends ArrayList<Integer>
{

    //~ Methods ----------------------------------------------------------------

    /**
     * Returns the contents of this list as an array of primitive {@code int}
     * values.
     *
     * @return List as int array.
     */
    public int [] toIntArray()
    {
        return toArray(this);
    }

    /**
     * Converts a list of {@link Integer} objects to an array of primitive
     * <code>int</code>s.
     *
     * @param integers List of Integer objects
     *
     * @return Array of primitive <code>int</code>s
     */
    public static int [] toArray(List<Integer> integers)
    {
        final int [] ints = new int[integers.size()];
        for (int i = 0; i < ints.length; i++) {
            ints[i] = integers.get(i);
        }
        return ints;
    }

    /**
     * Returns a list backed by an array of primitive <code>int</code> values.
     *
     * <p>The behavior is analogous to {@link Arrays#asList(Object[])}. Changes
     * to the list are reflected in the array. The list cannot be extended.
     *
     * @param args Array of primitive <code>int</code> values
     *
     * @return List backed by array
     */
    public static List<Integer> of(final int... args)
    {
        // use a shared constant for a common case
        if (args.length == 0) {
            return Collections.emptyList();
        }
        return new PrimitiveIntList(args);
    }

    /**
     * Returns an immutable list backed by an array of primitive
     * <code>int</code> values.
     *
     * <p>The behavior is analogous to {@link Arrays#asList(Object[])}. Changes
     * to the backing array are reflected in the list. The list cannot be
     * modified or extended.
     *
     * @param args Array of primitive <code>int</code> values
     *
     * @return Immutable list backed by array
     */
    public static List<Integer> ofImmutable(final int... args)
    {
        switch (args.length) {
        case 0:
            return Collections.emptyList();
        case 1:
            // Singleton list is immutable and holds an Integer. Values between
            // 0 and 255 are cached by the Java, therefore require less memory
            // than an int[1]. Other values require an Integer, which is about
            // the same size as an int[1].
            return Collections.singletonList(args[0]);
        default:
            return new ImmutablePrimitiveIntList(args);
        }
    }

    /**
     * Returns an immutable list backed by an array of primitive
     * <code>int</code> values whose contents are the given list.
     *
     * <p>The list is very space-efficient.
     *
     * @param argList List of integers
     *
     * @return Immutable list of integers
     */
    public static List<Integer> ofImmutable(List<Integer> argList)
    {
        switch (argList.size()) {
        case 0:
            return Collections.emptyList();
        case 1:
            // Singleton list is immutable and holds an Integer. Values between
            // 0 and 255 are cached by the Java, therefore require less memory
            // than an int[1]. Other values require an Integer, which is about
            // the same size as an int[1].
            return Collections.singletonList(argList.get(0));
        default:
            return new ImmutablePrimitiveIntList(toArray(argList));
        }
    }

    /**
     * List backed by an array of primitive {@code int} values.
     */
    private static class PrimitiveIntList extends AbstractList<Integer>
    {
        private final int[] args;

        /**
         * Creates a PrimitiveIntList.
         *
         * @param args Array of integers
         */
        protected PrimitiveIntList(int[] args)
        {
            this.args = args;
        }

        public Integer get(int index)
        {
            return args[index];
        }

        public int size()
        {
            return args.length;
        }

        public Integer set(int index, Integer element)
        {
            final int previous = args[index];
            args[index] = element;
            return previous;
        }
    }

    /**
     * List backed by an array of primitive {@code int} values.
     */
    private static final class ImmutablePrimitiveIntList
        extends PrimitiveIntList
    {
        /**
         * Creates an ImmutablePrimitiveIntList.
         *
         * @param args Array of integers
         */
        private ImmutablePrimitiveIntList(int[] args)
        {
            super(args);
        }

        public Integer set(int index, Integer element)
        {
            throw new UnsupportedOperationException("list is not mutable");
        }
    }
}

// End IntList.java

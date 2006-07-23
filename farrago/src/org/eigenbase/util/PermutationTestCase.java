/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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

import junit.framework.*;


/**
 * Unit test for {@link Permutation}.
 *
 * @author Julian Hyde
 * @version $Id$
 */
public class PermutationTestCase
    extends TestCase
{

    //~ Constructors -----------------------------------------------------------

    public PermutationTestCase(String name)
    {
        super(name);
    }

    //~ Methods ----------------------------------------------------------------

    public void testOne()
    {
        final Permutation perm = new Permutation(4);
        assertEquals(
            "[0, 1, 2, 3]",
            perm.toString());
        assertEquals(
            4,
            perm.size());

        perm.set(0, 2);
        assertEquals(
            "[2, 1, 0, 3]",
            perm.toString());

        perm.set(1, 0);
        assertEquals(
            "[2, 0, 1, 3]",
            perm.toString());

        final Permutation invPerm = perm.inverse();
        assertEquals(
            "[1, 2, 0, 3]",
            invPerm.toString());

        // changing perm doesn't change inverse
        perm.set(0, 0);
        assertEquals(
            "[0, 2, 1, 3]",
            perm.toString());
        assertEquals(
            "[1, 2, 0, 3]",
            invPerm.toString());
    }

    public void testTwo()
    {
        final Permutation perm = new Permutation(new int[] { 3, 2, 0, 1 });
        assertFalse(perm.isIdentity());
        assertEquals(
            "[3, 2, 0, 1]",
            perm.toString());

        Permutation perm2 = (Permutation) perm.clone();
        assertEquals(
            "[3, 2, 0, 1]",
            perm2.toString());
        assertTrue(perm.equals(perm2));
        assertTrue(perm2.equals(perm));

        perm.set(2, 1);
        assertEquals(
            "[3, 2, 1, 0]",
            perm.toString());
        assertFalse(perm.equals(perm2));

        // clone not affected
        assertEquals(
            "[3, 2, 0, 1]",
            perm2.toString());

        perm2.set(2, 3);
        assertEquals(
            "[0, 2, 3, 1]",
            perm2.toString());
    }

    public void testInsert()
    {
        Permutation perm = new Permutation(new int[] { 3, 0, 4, 2, 1 });
        perm.insertTarget(2);
        assertEquals(
            "[4, 0, 5, 3, 1, 2]",
            perm.toString());

        // insert at start
        perm = new Permutation(new int[] { 3, 0, 4, 2, 1 });
        perm.insertTarget(0);
        assertEquals(
            "[4, 1, 5, 3, 2, 0]",
            perm.toString());

        // insert at end
        perm = new Permutation(new int[] { 3, 0, 4, 2, 1 });
        perm.insertTarget(5);
        assertEquals(
            "[3, 0, 4, 2, 1, 5]",
            perm.toString());

        // insert into empty
        perm = new Permutation(new int[] {});
        perm.insertTarget(0);
        assertEquals(
            "[0]",
            perm.toString());
    }

    public void testEmpty()
    {
        final Permutation perm = new Permutation(0);
        assertTrue(perm.isIdentity());
        assertEquals(
            "[]",
            perm.toString());
        assertTrue(perm.equals(perm));
        assertTrue(perm.equals(perm.inverse()));

        try {
            perm.set(1, 0);
            fail("expected exception");
        } catch (ArrayIndexOutOfBoundsException e) {
            // success
        }

        try {
            perm.set(-1, 2);
            fail("expected exception");
        } catch (ArrayIndexOutOfBoundsException e) {
            // success
        }
    }
}

// End PermutationTestCase.java

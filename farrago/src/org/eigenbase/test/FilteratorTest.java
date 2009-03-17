/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006-2007 The Eigenbase Project
// Copyright (C) 2004-2007 Disruptive Tech
// Copyright (C) 2006-2007 LucidEra, Inc.
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

import junit.framework.*;

import org.eigenbase.util.*;


/**
 * Unit test for {@link Filterator}.
 *
 * @author jhyde
 * @version $Id$
 * @since September 6, 2006
 */
public class FilteratorTest
    extends TestCase
{
    //~ Methods ----------------------------------------------------------------

    public void testOne()
    {
        final List<String> tomDickHarry = Arrays.asList("tom", "dick", "harry");
        final Filterator<String> filterator =
            new Filterator<String>(tomDickHarry.iterator(), String.class);

        // call hasNext twice
        assertTrue(filterator.hasNext());
        assertTrue(filterator.hasNext());
        assertEquals("tom", filterator.next());

        // call next without calling hasNext
        assertEquals("dick", filterator.next());
        assertTrue(filterator.hasNext());
        assertEquals("harry", filterator.next());
        assertFalse(filterator.hasNext());
        assertFalse(filterator.hasNext());
    }

    public void testNulls()
    {
        // Nulls don't cause an error - but are not emitted, because they
        // fail the instanceof test.
        final List<String> tomDickHarry = Arrays.asList("paul", null, "ringo");
        final Filterator<String> filterator =
            new Filterator<String>(tomDickHarry.iterator(), String.class);
        assertEquals("paul", filterator.next());
        assertEquals("ringo", filterator.next());
        assertFalse(filterator.hasNext());
    }

    public void testSubtypes()
    {
        final ArrayList arrayList = new ArrayList();
        final HashSet hashSet = new HashSet();
        final LinkedList linkedList = new LinkedList();
        Collection [] collections =
        {
            null,
            arrayList,
            hashSet,
            linkedList,
            null,
        };
        final Filterator<List> filterator =
            new Filterator<List>(
                Arrays.asList(collections).iterator(),
                List.class);
        assertTrue(filterator.hasNext());

        // skips null
        assertTrue(arrayList == filterator.next());

        // skips the HashSet
        assertTrue(linkedList == filterator.next());
        assertFalse(filterator.hasNext());
    }

    public void testBox()
    {
        final Number [] numbers = { 1, 2, 3.14, 4, null, 6E23 };
        List<Integer> result = new ArrayList<Integer>();
        for (int i : Util.filter(Arrays.asList(numbers), Integer.class)) {
            result.add(i);
        }
        assertEquals("[1, 2, 4]", result.toString());
    }
}

// End FilteratorTest.java

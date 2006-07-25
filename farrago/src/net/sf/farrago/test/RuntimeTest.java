/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2003-2005 Disruptive Tech
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
package net.sf.farrago.test;

import java.util.*;

import junit.framework.*;

import net.sf.farrago.type.runtime.*;


/**
 * Unit tests for various classes in package {@link
 * net.sf.farrago.type.runtime}.
 *
 * @author jhyde
 * @version $Id$
 * @since Nov 11, 2004
 */
public class RuntimeTest
    extends TestCase
{

    //~ Methods ----------------------------------------------------------------

    /**
     * Test {@link CharStringComparator}.
     */
    public void testCharStringComparator()
    {
        assertEquals(
            0,
            CharStringComparator.compareCharStrings("x ", "x"));
        assertEquals(
            -1,
            CharStringComparator.compareCharStrings("a", "b"));
        assertEquals(
            1,
            CharStringComparator.compareCharStrings("aa", "a"));
        assertEquals(
            0,
            CharStringComparator.compareCharStrings("aa  ", "aa   "));
        assertEquals(
            2,
            CharStringComparator.compareCharStrings("aa a", "aa   "));
        assertEquals(
            0,
            CharStringComparator.compareCharStrings("", ""));
        assertEquals(
            0,
            CharStringComparator.compareCharStrings("", " "));
        assertEquals(
            0,
            CharStringComparator.compareCharStrings(" ", ""));
        assertEquals(
            1,
            CharStringComparator.compareCharStrings("a", ""));
        String [] beatles = { "john", "paul", "george", "", "ringo" };
        Arrays.sort(
            beatles,
            new CharStringComparator());
        assertEquals(
            ", george, john, paul, ringo",
            toString(beatles));
    }

    private String toString(String [] a)
    {
        StringBuffer buf = new StringBuffer();
        for (int i = 0; i < a.length; i++) {
            if (i > 0) {
                buf.append(", ");
            }
            buf.append(a[i]);
        }
        return buf.toString();
    }
}

// End RuntimeTest.java

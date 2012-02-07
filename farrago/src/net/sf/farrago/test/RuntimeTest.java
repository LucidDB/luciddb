/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
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

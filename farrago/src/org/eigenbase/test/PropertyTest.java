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

package org.eigenbase.test;

import junit.framework.TestCase;
import junit.framework.Assert;
import org.eigenbase.util.property.*;


/**
 * Unit test for properties system.
 *
 * @author jhyde
 * @since July 6, 2005
 * @version $Id$
 */
public class PropertyTest extends TestCase
 {
    public void testInt()
    {
        final MyProperties props = new MyProperties();

        // Default value.
        Assert.assertEquals(5, props.intProp.get());

        props.intProp.set(8);
        Assert.assertEquals(8, props.intProp.get());
    }

    public void testTrigger()
    {
        final MyProperties props = new MyProperties();
        final int[] ints = {0};
        props.intProp.addTrigger(
                new Trigger() {
                    public boolean isPersistent() {
                        return false;
                    }

                    public int phase() {
                        return 0;
                    }

                    public void execute(Property property, String value)
                            throws VetoRT {
                        int intValue = Integer.parseInt(value);
                        if (intValue > 10) {
                            ints[0] = intValue;
                        }
                        if (intValue > 100) {
                            throw new VetoRT("too big");
                        }
                    }
                }
        );
        props.intProp.set(5);
        assertEquals(0, ints[0]); // unchanged
        props.intProp.set(15);
        assertEquals(15, ints[0]); // changed by trigger
        try {
            props.intProp.set(120);
            fail("expecting exception");
        } catch (Trigger.VetoRT e) {
            assertEquals("too big", e.getMessage());
        }
        Assert.assertEquals(15, props.intProp.get()); // change was rolled back
    }

    private static class MyProperties extends TriggerableProperties {
        public final IntegerProperty intProp = new IntegerProperty(
                this, "props.int", 5);
    }
}

// End PropertyTest.java

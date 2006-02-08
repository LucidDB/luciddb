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

import org.eigenbase.util.property.*;

import junit.framework.Assert;
import junit.framework.TestCase;

/**
 * Unit test for properties system ({@link TriggerableProperties},
 * {@link IntegerProperty} and the like).
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
        Assert.assertEquals(789, props.intProp.get(789));

        int prev = props.intProp.set(8);
        Assert.assertEquals(8, props.intProp.get());
        Assert.assertEquals(5, prev);

        prev = props.intProp.set(0);
        Assert.assertEquals(8, prev);
    }

    public void testIntNoDefault()
    {
        final MyProperties props = new MyProperties();

        // As above, on property with no default value.
        Assert.assertEquals(0, props.intPropNoDefault.get());
        Assert.assertEquals(17, props.intPropNoDefault.get(17));

        int prev = props.intPropNoDefault.set(-56);
        Assert.assertEquals(0, prev);
        Assert.assertEquals(-56, props.intPropNoDefault.get());
        Assert.assertEquals(-56, props.intPropNoDefault.get(17));

        // Second time set returns the previous value.
        prev = props.intPropNoDefault.set(12345);
        Assert.assertEquals(-56, prev);

        // Setting null is not OK.
        try {
            props.intPropNoDefault.setString(null);
            fail("expected NPE");
        } catch (NullPointerException e) {
            // ok
        }
    }

    public void testDouble()
    {
        final MyProperties props = new MyProperties();

        // Default value.
        Assert.assertEquals(-3.14, props.doubleProp.get());
        Assert.assertEquals(.789, props.doubleProp.get(.789));

        double prev = props.doubleProp.set(.8);
        Assert.assertEquals(.8, props.doubleProp.get());
        Assert.assertEquals(-3.14, prev);

        prev = props.doubleProp.set(.0);
        Assert.assertEquals(.8, prev);
    }

    public void testDoubleNoDefault()
    {
        final MyProperties props = new MyProperties();

        // As above, on property with no default value.
        Assert.assertEquals(.0, props.doublePropNoDefault.get());
        Assert.assertEquals(.17, props.doublePropNoDefault.get(.17));

        double prev = props.doublePropNoDefault.set(-.56);
        Assert.assertEquals(.0, prev);
        Assert.assertEquals(-.56, props.doublePropNoDefault.get());
        Assert.assertEquals(-.56, props.doublePropNoDefault.get(.17));

        // Second time set returns the previous value.
        prev = props.doublePropNoDefault.set(.12345);
        Assert.assertEquals(-.56, prev);

        // Setting null is not OK.
        try {
            props.doublePropNoDefault.setString(null);
            fail("expected NPE");
        } catch (NullPointerException e) {
            // ok
        }
    }

    public void testString()
    {
        final MyProperties props = new MyProperties();

        // Default value.
        Assert.assertEquals("foo", props.stringProp.get());
        Assert.assertEquals("xxxxx", props.stringProp.get("xxxxx"));

        // First time set returns the default value.
        String prev = props.stringProp.set("bar");
        Assert.assertEquals("bar", props.stringProp.get());
        Assert.assertEquals("foo", prev);

        // Second time set returns the previous value.
        prev = props.stringProp.set("baz");
        Assert.assertEquals("bar", prev);

        // Setting null is not OK.
        try {
            prev = props.stringProp.set(null);
            fail("expected NPE");
        } catch (NullPointerException e) {
            // ok
        }
    }

    public void testStringNoDefault()
    {
        final MyProperties props = new MyProperties();

        // As above, on property with no default value.
        Assert.assertEquals(null, props.stringPropNoDefault.get());
        Assert.assertEquals("xx", props.stringPropNoDefault.get("xx"));

        String prev = props.stringPropNoDefault.set("paul");
        Assert.assertEquals(null, prev);
        Assert.assertEquals("paul", props.stringPropNoDefault.get());
        Assert.assertEquals("paul", props.stringPropNoDefault.get("xx"));

        // Second time set returns the previous value.
        prev = props.stringPropNoDefault.set("ringo");
        Assert.assertEquals("paul", prev);

        // Setting null is not OK.
        try {
            prev = props.stringPropNoDefault.set(null);
            fail("expected NPE");
        } catch (NullPointerException e) {
            // ok
        }

    }

    public void testBoolean()
    {
        final MyProperties props = new MyProperties();

        // Default value.
        Assert.assertEquals(true, props.booleanProp.get());
        Assert.assertEquals(false, props.booleanProp.get(false));

        // First time set returns the default value.
        boolean prev = props.booleanProp.set(false);
        Assert.assertEquals(false, props.booleanProp.get());
        Assert.assertEquals(true, prev);

        // Second time set returns the previous value.
        prev = props.booleanProp.set(true);
        Assert.assertEquals(false, prev);

        // Various values all mean true.
        String prevString = props.booleanProp.setString("1");
        Assert.assertEquals(true, props.booleanProp.get());
        prevString = props.booleanProp.setString("true");
        Assert.assertEquals(true, props.booleanProp.get());
        prevString = props.booleanProp.setString("TRUE");
        Assert.assertEquals(true, props.booleanProp.get());
        prevString = props.booleanProp.setString("yes");
        Assert.assertEquals(true, props.booleanProp.get());
        prevString = props.booleanProp.setString("Yes");
        Assert.assertEquals(true, props.booleanProp.get());

        // Leading and trailing spaces are ignored.
        prevString = props.booleanProp.setString("  yes  ");
        Assert.assertEquals(true, props.booleanProp.get());
        prevString = props.booleanProp.setString("false   ");
        Assert.assertEquals(false, props.booleanProp.get());
        prevString = props.booleanProp.setString("true ");
        Assert.assertEquals(true, props.booleanProp.get());

        // All other values mean false.
        prevString = props.booleanProp.setString("");
        Assert.assertEquals(false, props.booleanProp.get());
        prevString = props.booleanProp.setString("no");
        Assert.assertEquals(false, props.booleanProp.get());
        prevString = props.booleanProp.setString("wombat");
        Assert.assertEquals(false, props.booleanProp.get());
        prevString = props.booleanProp.setString("0");
        Assert.assertEquals(false, props.booleanProp.get());
        prevString = props.booleanProp.setString("false");
        Assert.assertEquals(false, props.booleanProp.get());

        // Setting null is not OK.
        try {
            props.booleanProp.setString(null);
            fail("expected NPE");
        } catch (NullPointerException e) {
            // ok
        }
    }

    public void testBooleanNoDefault()
    {
        final MyProperties props = new MyProperties();

        // As above, on property with no default value.
        Assert.assertEquals(false, props.booleanPropNoDefault.get());
        Assert.assertEquals(true, props.booleanPropNoDefault.get(true));
        Assert.assertEquals(false, props.booleanPropNoDefault.get(false));

        boolean prev = props.booleanPropNoDefault.set(true);
        Assert.assertEquals(false, prev);
        Assert.assertEquals(true, props.booleanPropNoDefault.get());
        Assert.assertEquals(true, props.booleanPropNoDefault.get(false));

        // Second time set returns the previous value.
        prev = props.booleanPropNoDefault.set(false);
        Assert.assertEquals(true, prev);

        // Setting null is not OK.
        try {
            props.booleanPropNoDefault.setString(null);
            fail("expected NPE");
        } catch (NullPointerException e) {
            // ok
        }

    }

    public void testTrigger()
    {
        final MyProperties props = new MyProperties();
        final int[] ints = {0};
        final Trigger trigger = new Trigger() {
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
        };
        props.intProp.addTrigger(trigger);
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

    private static class State
    {
        boolean triggerCalled;
        String triggerValue;
    }

    /**
     * Tests that trigger is called after the value is changed
     */
    public void testValueChange()
    {
        final MyProperties props = new MyProperties();

        String path= "test.mondrian.properties.change.value";
        BooleanProperty boolProp = new BooleanProperty(
            props,
            path,
            false);

        assertTrue("Check property value NOT false",
            (! boolProp.get()));

        // set via the 'set' method
        final boolean prevBoolean = boolProp.set(true);
        assertEquals(false, prevBoolean);

        // now explicitly set the property
        final Object prevObject = props.setProperty(path, "false");
        assertEquals("true", prevObject);

        String v = props.getProperty(path);
        assertTrue("Check property value is null",
            (v != null));
        assertTrue("Check property value is true",
            (! Boolean.valueOf(v).booleanValue()));

        final State state = new State();
        state.triggerCalled = false;
        state.triggerValue = null;

        final Trigger trigger = new Trigger() {
            public boolean isPersistent() {
                return false;
            }
            public int phase() {
                return Trigger.PRIMARY_PHASE;
            }
            public void execute(Property property, String value) {
                state.triggerCalled = true;
                state.triggerValue = value;
            }
        };
        boolProp.addTrigger(trigger);

        String falseStr = "false";
        props.setProperty(path, falseStr);
        assertTrue("Check trigger was called", ! state.triggerCalled);

        String trueStr = "true";
        props.setProperty(path, trueStr);

        assertTrue("Check trigger was NOT called",
            state.triggerCalled);
        assertTrue("Check trigger value was null",
            (state.triggerValue != null));
        assertTrue("Check trigger value is NOT correct",
            state.triggerValue.equals(trueStr));

    }

    private static class State2
    {
        int callCounter;
        int primaryOne;
        int primaryTwo;
        int secondaryOne;
        int secondaryTwo;
        int tertiaryOne;
        int tertiaryTwo;
    }

    /**
     * Checks that triggers are called in the correct order.
     */
    public void testTriggerCallOrder()
    {
        final MyProperties props = new MyProperties();
        String path= "test.mondrian.properties.call.order";
        BooleanProperty boolProp = new BooleanProperty(
            props,
            path,
            false);

        final State2 state = new State2();
        state.callCounter = 0;

        // now explicitly set the property
        props.setProperty(path, "false");

        String v = props.getProperty(path);
        assertTrue("Check property value is null",
            (v != null));
        assertTrue("Check property value is true",
            (! Boolean.valueOf(v).booleanValue()));

        // primaryOne
        Trigger primaryOneTrigger =
            new Trigger() {
                public boolean isPersistent() {
                    return false;
                }
                public int phase() {
                    return Trigger.PRIMARY_PHASE;
                }
                public void execute(Property property, String value) {
                    state.primaryOne = state.callCounter++;
                }
            };
        boolProp.addTrigger(primaryOneTrigger);

        // secondaryOne
        Trigger secondaryOneTrigger =
            new Trigger() {
                public boolean isPersistent() {
                    return false;
                }
                public int phase() {
                    return Trigger.SECONDARY_PHASE;
                }
                public void execute(Property property, String value) {
                    state.secondaryOne = state.callCounter++;
                }
            };
        boolProp.addTrigger(secondaryOneTrigger);

        // tertiaryOne
        Trigger tertiaryOneTrigger =
            new Trigger() {
                public boolean isPersistent() {
                    return false;
                }
                public int phase() {
                    return Trigger.TERTIARY_PHASE;
                }
                public void execute(Property property, String value) {
                    state.tertiaryOne = state.callCounter++;
                }
            };
        boolProp.addTrigger(tertiaryOneTrigger);

        // tertiaryTwo
        Trigger tertiaryTwoTrigger =
            new Trigger() {
                public boolean isPersistent() {
                    return false;
                }
                public int phase() {
                    return Trigger.TERTIARY_PHASE;
                }
                public void execute(Property property, String value) {
                    state.tertiaryTwo = state.callCounter++;
                }
            };
        boolProp.addTrigger(tertiaryTwoTrigger);

        // secondaryTwo
        Trigger secondaryTwoTrigger =
            new Trigger() {
                public boolean isPersistent() {
                    return false;
                }
                public int phase() {
                    return Trigger.SECONDARY_PHASE;
                }
                public void execute(Property property, String value) {
                    state.secondaryTwo = state.callCounter++;
                }
            };
        boolProp.addTrigger(secondaryTwoTrigger);

        // primaryTwo
        Trigger primaryTwoTrigger =
            new Trigger() {
                public boolean isPersistent() {
                    return false;
                }
                public int phase() {
                    return Trigger.PRIMARY_PHASE;
                }
                public void execute(Property property, String value) {
                    state.primaryTwo = state.callCounter++;
                }
            };
        boolProp.addTrigger(primaryTwoTrigger);

        String falseStr = "false";
        props.setProperty(path, falseStr);
        assertTrue("Check trigger was called",
            (state.callCounter == 0));

        String trueStr = "true";
        props.setProperty(path, trueStr);

        assertTrue("Check trigger was NOT called",
            (state.callCounter != 0 ));
        assertTrue("Check triggers was NOT called correct number of times",
            (state.callCounter == 6 ));

        // now make sure that primary are called before secondary which are
        // before tertiary
        assertTrue("Check primaryOne > secondaryOne",
            (state.primaryOne < state.secondaryOne));
        assertTrue("Check primaryOne > secondaryTwo",
            (state.primaryOne < state.secondaryTwo));
        assertTrue("Check primaryOne > tertiaryOne",
            (state.primaryOne < state.tertiaryOne));
        assertTrue("Check primaryOne > tertiaryTwo",
            (state.primaryOne < state.tertiaryTwo));

        assertTrue("Check primaryTwo > secondaryOne",
            (state.primaryTwo < state.secondaryOne));
        assertTrue("Check primaryTwo > secondaryTwo",
            (state.primaryTwo < state.secondaryTwo));
        assertTrue("Check primaryTwo > tertiaryOne",
            (state.primaryTwo < state.tertiaryOne));
        assertTrue("Check primaryTwo > tertiaryTwo",
            (state.primaryTwo < state.tertiaryTwo));

        assertTrue("Check secondaryOne > tertiaryOne",
            (state.secondaryOne < state.tertiaryOne));
        assertTrue("Check secondaryOne > tertiaryTwo",
            (state.secondaryOne < state.tertiaryTwo));

        assertTrue("Check secondaryTwo > tertiaryOne",
            (state.secondaryTwo < state.tertiaryOne));
        assertTrue("Check secondaryTwo > tertiaryTwo",
            (state.secondaryTwo < state.tertiaryTwo));



        // remove some of the triggers
        boolProp.removeTrigger(primaryTwoTrigger);
        boolProp.removeTrigger(secondaryTwoTrigger);
        boolProp.removeTrigger(tertiaryTwoTrigger);

        // reset
        state.callCounter = 0;
        state.primaryOne = 0;
        state.primaryTwo = 0;
        state.secondaryOne = 0;
        state.secondaryTwo = 0;
        state.tertiaryOne = 0;
        state.tertiaryTwo = 0;

        props.setProperty(path, falseStr);
        assertTrue("Check trigger was NOT called",
            (state.callCounter != 0 ));
        assertTrue("Check triggers was NOT called correct number of times",
            (state.callCounter == 3 ));

        // now make sure that primary are called before secondary which are
        // before tertiary
        assertTrue("Check primaryOne > secondaryOne",
            (state.primaryOne < state.secondaryOne));
        assertTrue("Check primaryOne > tertiaryOne",
            (state.primaryOne < state.tertiaryOne));

        assertTrue("Check secondaryOne > tertiaryOne",
            (state.secondaryOne < state.tertiaryOne));

    }

    private static class State3
    {
        int callCounter;
        boolean triggerCalled;
        String triggerValue;
    }

    /**
     * Checks that one can veto a property change.
     */
    public void testVetoChangeValue() throws Exception
    {
        final MyProperties props = new MyProperties();
        String path= "test.mondrian.properties.veto.change.value";
        IntegerProperty intProp = new IntegerProperty(
            props,
            path,
            -1);

        assertTrue("Check property value NOT false",
            (intProp.get() == -1));

        // now explicitly set the property
        props.setProperty(path, "-1");

        String v = props.getProperty(path);
        assertTrue("Check property value is null",
            (v != null));

        assertTrue("Check property value is -1",
            (Integer.decode(v).intValue() == -1));

        final State3 state = new State3();
        state.callCounter = 0;

        // Add a trigger. Keep it on the stack to prevent it from being
        // garbage-collected.
        final Trigger trigger1 = new Trigger() {
            public boolean isPersistent() {
                return false;
            }
            public int phase() {
                return Trigger.PRIMARY_PHASE;
            }
            public void execute(Property property, String value) {
                state.triggerCalled = true;
                state.triggerValue = value;
            }
        };
        intProp.addTrigger(trigger1);

        final Trigger trigger2 = new Trigger() {
            public boolean isPersistent() {
                return false;
            }
            public int phase() {
                return Trigger.SECONDARY_PHASE;
            }
            public void execute(Property property, String value)
                throws VetoRT {

                // even numbers are rejected
                state.callCounter++;
                int ival = Integer.decode(value).intValue();
                if ((ival % 2) == 0) {
                    // throw on even
                    throw new VetoRT("have a nice day");
                } else {
                    // ok
                }
            }
        };
        intProp.addTrigger(trigger2);

        for (int i = 0; i < 10; i++) {
            // reset values
            state.triggerCalled = false;
            state.triggerValue = null;

            boolean isEven = ((i % 2) == 0);

            try {
                props.setProperty(path,
                    Integer.toString(i));
            } catch (Trigger.VetoRT ex) {
                // Trigger rejects even numbers so if even its ok
                if (! isEven) {
                    fail("Did not reject even number: " +i);
                }
                int val = Integer.decode(state.triggerValue).intValue();

                // the property value was reset to the previous value of "i"
                // so we add "1" to it to get the current value.
                assertTrue("Even counter not value plus one", (i == val+1));
                continue;
            }
            // should only be here if odd
            if (isEven) {
                fail("Did not pass odd number: " +i);
            }
            int val = Integer.decode(state.triggerValue).intValue();

            assertTrue("Odd counter not value", (i == val));
        }

    }

    /**
     * Runs {@link #testVetoChangeValue} many times, to test concurrency.
     */
    public void testVetoChangeValueManyTimes() throws Exception
    {
        for (int i = 0; i < 1000; ++i) {
            testVetoChangeValue();
        }
    }

    private static class MyProperties extends TriggerableProperties
    {
        public final IntegerProperty intProp = new IntegerProperty(
            this, "props.int", 5);

        public final IntegerProperty intPropNoDefault =
            new IntegerProperty(this, "props.int.nodefault");

        public final StringProperty stringProp =
            new StringProperty(this, "props.string", "foo");

        public final StringProperty stringPropNoDefault =
            new StringProperty(this, "props.string.nodefault", null);

        public final DoubleProperty doubleProp =
            new DoubleProperty(this, "props.double", -3.14);

        public final DoubleProperty doublePropNoDefault =
            new DoubleProperty(this, "props.double.nodefault");

        public final BooleanProperty booleanProp =
            new BooleanProperty(this, "props.boolean", true);

        public final BooleanProperty booleanPropNoDefault =
            new BooleanProperty(this, "props.boolean.nodefault");
    }
}

// End PropertyTest.java

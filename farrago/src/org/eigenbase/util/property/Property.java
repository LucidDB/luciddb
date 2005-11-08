/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
//
// This library is free software; you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by the
// Free Software Foundation; either version 2 of the License, or (at your
// option) any later version approved by The Eigenbase Project.
//
// This library is distributed in the hope that it will be useful, 
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public License
// along with this library; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/

package org.eigenbase.util.property;

import java.util.*;
import java.lang.ref.WeakReference;


/**
 * Definition and accessor for a property.
 *
 * <p>For example:
 * <blockquote><code><pre>
 * class MyProperties extends Properties {
 *     public final IntegerProperty DebugLevel =
 *         new IntegerProperty(this, "com.acme.debugLevel", 10);
 * }
 *
 * MyProperties props = new MyProperties();
 * System.out.println(props.DebugLevel.get()); // prints "10", the default
 * props.DebugLevel.set(20);
 * System.out.println(props.DebugLevel.get()); // prints "20"
 * </pre></code></blockquote>
 *
 * @author jhyde
 * @since May 4, 2004
 * @version $Id$
 **/
public abstract class Property
{
    //~ Instance fields -------------------------------------------------------

    protected final Properties properties;
    private final String path;
    private final String defaultValue;
    private final TriggerList triggerList = new TriggerList();

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a Property and associates it with an underlying properties
     * object.
     *
     * @param properties Properties object which holds values for this
     *    property.
     * @param path Name by which this property is serialized to a properties
     *    file, for example "com.acme.trace.Verbosity".
     * @param defaultValue Default value, null if there is no default.
     */
    protected Property(
        Properties properties,
        String path,
        String defaultValue)
    {
        this.properties = properties;
        this.path = path;
        this.defaultValue = defaultValue;
        if (properties instanceof TriggerableProperties) {
            ((TriggerableProperties) properties).register(this);
        }
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * @return this property's name (typically a dotted path)
     */
    public String getPath()
    {
        return path;
    }

    /**
     * Returns the default value of this property. Derived classes (for example
     * those with special rules) can override.
     */
    public String getDefaultValue()
    {
        return defaultValue;
    }

    /**
     * Retrieves the value of a property, using a given default value, and
     * optionally failing if there is no value.
     */
    protected String getInternal(
        String defaultValue,
        boolean required)
    {
        String value = properties.getProperty(path, defaultValue);
        if (value != null) {
            return value;
        }
        if (defaultValue == null) {
            value = getDefaultValue();
            if (value != null) {
                return value;
            }
        }
        if (required) {
            throw new RuntimeException("Property " + path + " must be set");
        }
        return value;
    }

    /**
     * Adds a trigger to this property.
     */
    public void addTrigger(Trigger trigger) {
        triggerList.add(trigger);
    }

    /**
     * Removes a trigger from this property.
     */
    public void removeTrigger(Trigger trigger)
    {
        triggerList.remove(trigger);
    }

    /**
     * Called when a property's value has just changed.
     *
     * <p>If one of the triggers on the property throws a
     * {@link org.eigenbase.util.property.Trigger.VetoRT} exception, this
     * method passes it on.
     *
     * @param oldValue Previous value of the property
     * @param value New value of the property
     *
     * @throws org.eigenbase.util.property.Trigger.VetoRT if one of the
     *   triggers threw a VetoRT
     */
    public void onChange(String oldValue, String value) {
        if (TriggerableProperties.equals(oldValue, value)) {
            return;
        }

        triggerList.execute(this, value);
    }

    /**
     * Sets a property directly as a string.
     */
    public Object setString(String value)
    {
        return properties.setProperty(path, value);
    }

    /**
     * Returns whether this property has a value assigned.
     */
    public boolean isSet()
    {
        return properties.get(path) != null;
    }

    /**
     * Returns the value of this property as a string.
     */
    public String getString()
    {
        return (String) properties.getProperty(path, defaultValue);
    }

    /**
     * Returns the boolean value of this property.
     */
    public boolean booleanValue() {
        final String value = getInternal(null, false);
        if (value == null) {
            return false;
        }
        return toBoolean(value);
    }

    protected static boolean toBoolean(final String value)
    {
        return value.equalsIgnoreCase("1") ||
                value.equalsIgnoreCase("true") ||
                value.equalsIgnoreCase("yes");
    }

    /**
     * Returns the value of the property as a string.
     */
    public String stringValue()
    {
        return getInternal(null, false);
    }

    /**
     * A trigger list is associated with a property key.
     * Each contains zero or more Triggers.
     * Each Trigger is stored in a WeakReference so that
     * when the the Trigger is only reachable via weak referencs the Trigger
     * will be be collected and the contents of the WeakReference
     * will be set to null.
     */
    private static class TriggerList extends ArrayList
    {
        /**
         * Add a Trigger wrapping it in a WeakReference.
         *
         * @param trigger
         */
        void add(final Trigger trigger) {
            // this is the object to add to list
            Object o = (trigger.isPersistent())
                        ? trigger : (Object) new WeakReference(trigger);

            // Add a Trigger in the correct group of phases in the list
            for (ListIterator it = listIterator(); it.hasNext(); ) {
                Trigger t = convert(it.next());

                if (t == null) {
                    it.remove();
                } else if (trigger.phase() < t.phase()) {
                    // add it before
                    it.hasPrevious();
                    it.add(o);
                    return;
                } else if (trigger.phase() == t.phase()) {
                    // add it after
                    it.add(o);
                    return;
                }
            }
            super.add(o);
        }

        /**
         * Remove the given Trigger.
         * In addition, any WeakReference that is empty are removed.
         *
         * @param trigger
         */
        void remove(final Trigger trigger) {
            for (Iterator it = iterator(); it.hasNext(); ) {
                Trigger t = convert(it.next());

                if (t == null) {
                    it.remove();
                } else if (t.equals(trigger)) {
                    it.remove();
                }
            }
        }

        /**
         * Execute all Triggers in this Entry passing in the property
         * key whose change was the casue.
         * In addition, any WeakReference that is empty are removed.
         *
         * @param property The property whose change caused this property to
         *   fire
         */
        void execute(Property property, String value) throws Trigger.VetoRT
        {
            // Make a copy so that if during the execution of a trigger a
            // Trigger is added or removed, we do not get a concurrent
            // modification exception. We do an explicit copy (rather than
            // a clone) so that we can remove any WeakReference whose
            // content has become null.
            List l = new ArrayList();
            for (Iterator it = iterator(); it.hasNext(); ) {
                Trigger t = convert(it.next());

                if (t == null) {
                    it.remove();
                } else {
                    l.add(t);
                }
            }

            for (Iterator it = l.iterator(); it.hasNext(); ) {
                Trigger t = (Trigger) it.next();
                t.execute(property, value);
            }
        }

        private Trigger convert(Object o) {
            if (o instanceof WeakReference) {
                o = ((WeakReference) o).get();
            }
            return (Trigger) o;
        }
    }
}

// End Property.java

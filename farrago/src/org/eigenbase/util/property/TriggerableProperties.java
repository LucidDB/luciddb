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
package org.eigenbase.util.property;

import java.util.*;

/**
 * Base class for properties which can respond to triggers.
 *
 * <p>If you wish to be notified of changes to properties, use the
 * {@link Property#addTrigger(Trigger)} method to register a callback.
 *
 * @author Julian Hyde
 * @since 5 July 2005
 * @version $Id$
 */
public class TriggerableProperties extends Properties
{
    protected final Map triggers = new HashMap();
    protected final Map properties = new HashMap();

    protected TriggerableProperties() {
    }

    /**
     * Sets the value of a property.
     *
     * <p>If the previous value does not equal the new value, executes any
     * {@link Trigger}s associated with the property, in order of their
     * {@link Trigger#phase() phase}.
     *
     * @param key
     * @param value
     * @return the old value
     */
    public synchronized Object setProperty(
            final String key, final String value)
    {
        String oldValue = super.getProperty(key);
        Object object = super.setProperty(key, value);
        if (oldValue == null && object != null) {
            oldValue = object.toString();
        }

        // If there is a property object, notify it to give it chane to fire
        // its triggers. If one of those triggers fires a veto exception, roll
        // back the change.
        Property property = (Property) properties.get(key);
        if (property != null &&
                triggersAreEnabled()) {
            try {
                property.onChange(oldValue, value);
            } catch (Trigger.VetoRT vex) {
                // Reset to the old value, do not call setProperty
                // unless you want to run out of stack space!
                superSetProperty(key, oldValue);
                try {
                    property.onChange(value, oldValue);
                } catch (Trigger.VetoRT ex) {
                    // ignore during reset
                }
                // Re-throw.
                throw vex;
            }
        }
        return oldValue;
    }

    /**
     * Whether triggers are enabled.
     * Derived class can override.
     */
    public boolean triggersAreEnabled()
    {
        return true;
    }

    /**
     * This is ONLY called during a veto
     * operation. It calls the super class {@link #setProperty}.
     *
     * @param key
     * @param oldValue
     */
    private void superSetProperty(String key, String oldValue) {
        if (oldValue != null) {
            super.setProperty(key, oldValue);
        }
    }

    static boolean equals(Object o1, Object o2)
    {
        return o1 == null ?
                o2 == null :
                o2 != null && o1.equals(o2);
    }

    /**
     * Registers a property with this properties object to make it available
     * for callbacks.
     */
    public void register(Property property)
    {
        properties.put(property.getPath(), property);
    }

    /**
     * Returns an array of registered properties.
     */
    public Property[] getProperties()
    {
        final Collection propertyList = properties.values();
        return (Property[]) propertyList.toArray(
                new Property[propertyList.size()]);
    }
}

// End TriggerableProperties.java

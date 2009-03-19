/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2002-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 2003-2009 John V. Sichi
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


/**
 * Definition and accessor for a boolean property.
 *
 * @author jhyde
 * @version $Id$
 * @since May 4, 2004
 */
public class BooleanProperty
    extends Property
{
    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a Boolean property.
     *
     * @param properties Properties object which holds values for this property.
     * @param path Name by which this property is serialized to a properties
     * file, for example "com.acme.trace.Verbosity".
     * @param defaultValue Default value, null if there is no default.
     */
    public BooleanProperty(
        Properties properties,
        String path,
        boolean defaultValue)
    {
        super(properties, path, defaultValue ? "true" : "false");
    }

    /**
     * Creates a Boolean property which has no default value.
     *
     * @param properties Properties object which holds values for this property.
     * @param path Name by which this property is serialized to a properties
     * file, for example "com.acme.trace.Verbosity".
     */
    public BooleanProperty(
        Properties properties,
        String path)
    {
        super(properties, path, null);
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Retrieves the value of this boolean property.
     *
     * <p>Returns <code>true</code> if the property exists, and its value is
     * <code>1</code>, <code>true</code> or <code>yes</code>; returns <code>
     * false</code> otherwise.
     */
    public boolean get()
    {
        return booleanValue();
    }

    /**
     * Retrieves the value of this boolean property.
     *
     * <p>Returns <code>true</code> if the property exists, and its value is
     * <code>1</code>, <code>true</code> or <code>yes</code>; returns <code>
     * false</code> otherwise.
     */
    public boolean get(boolean defaultValue)
    {
        final String value =
            getInternal(
                Boolean.toString(defaultValue),
                false);
        if (value == null) {
            return defaultValue;
        }
        return toBoolean(value);
    }

    /**
     * Sets the value of this boolean property.
     *
     * @return The previous value, or the default value if not set.
     */
    public boolean set(boolean value)
    {
        String prevValue = setString(Boolean.toString(value));
        if (prevValue == null) {
            prevValue = getDefaultValue();
            if (prevValue == null) {
                return false;
            }
        }
        return toBoolean(prevValue);
    }
}

// End BooleanProperty.java

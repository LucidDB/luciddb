/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2002 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 2003 John V. Sichi
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
 * Definition and accessor for a string property.
 *
 * @author jhyde
 * @version $Id$
 * @since May 4, 2004
 */
public class StringProperty
    extends Property
{
    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a string property.
     *
     * @param properties Properties object which holds values for this property.
     * @param path Name by which this property is serialized to a properties
     * file, for example "com.acme.trace.Verbosity".
     * @param defaultValue Default value, null if there is no default.
     */
    public StringProperty(
        Properties properties,
        String path,
        String defaultValue)
    {
        super(properties, path, defaultValue);
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Retrieves the value of this property. Returns the property's default
     * value if the property set has no value for this property.
     */
    public String get()
    {
        return stringValue();
    }

    /**
     * Retrieves the value of this property, optionally failing if there is no
     * value. Returns the property's default value if the property set has no
     * value for this property.
     */
    public String get(boolean required)
    {
        return getInternal(null, required);
    }

    /**
     * Retrieves the value of this property, or the default value if none is
     * found.
     */
    public String get(String defaultValue)
    {
        return getInternal(defaultValue, false);
    }

    /**
     * Sets the value of this property.
     *
     * @return The previous value, or the default value if not set.
     */
    public String set(String value)
    {
        String prevValue = setString(value);
        if (prevValue == null) {
            prevValue = getDefaultValue();
        }
        return prevValue;
    }
}

// End StringProperty.java

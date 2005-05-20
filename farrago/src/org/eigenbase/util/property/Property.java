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

import java.util.Properties;

import org.eigenbase.util.Util;


/**
 * Definition and accessor for a property.
 *
 * <p>For example:
 *
 * @author jhyde
 * @since May 4, 2004
 * @version $Id$
 **/
public abstract class Property
{
    //~ Instance fields -------------------------------------------------------

    protected final Properties properties;
    public final String path;
    private final String defaultValue;

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
    protected String getDefaultValue()
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
            throw Util.newInternal("Property " + path + " must be set");
        }
        return value;
    }
}


// End Property.java

/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2004 Disruptive Technologies, Inc.
// You must accept the terms in LICENSE.html to use this software.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/
package org.eigenbase.util.property;

import java.util.Properties;

/**
 * Definition and accessor for a string property.
 *
 * @author jhyde
 * @since May 4, 2004
 * @version $Id$
 **/
public class StringProperty extends Property {
    /**
     * Creates a string property.
     *
     * @param properties Properties object which holds values for this
     *    property.
     * @param path Name by which this property is serialized to a properties
     *    file, for example "com.acme.trace.Verbosity".
     * @param defaultValue Default value, null if there is no default.
     */
    public StringProperty(Properties properties, String path,
            String defaultValue) {
        super(properties, path, defaultValue);
    }

    /**
     * Retrieves the value of this property. Returns the property's default
     * value if the property set has no value for this property.
     */
    public String get() {
        return getInternal(null, false);
    }

    /**
     * Retrieves the value of this property, optionally failing if there is
     * no value. Returns the property's default value if the property set has
     * no value for this property.
     */
    public String get(boolean required) {
        return getInternal(null, required);
    }

    /**
     * Retrieves the value of this property, or the default value if none is
     * found.
     */
    public String get(String defaultValue) {
        return getInternal(defaultValue, false);
    }

    /**
     * Sets the value of this property.
     */
    public void set(String value) {
        _properties.setProperty(_path, value);
    }
}

// End StringProperty.java

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
package net.sf.saffron.util.property;

import java.util.Properties;

/**
 * Definition and accessor for an integer property.
 *
 * @author jhyde
 * @since May 4, 2004
 * @version $Id$
 **/
public class IntegerProperty extends Property {
    /**
     * Creates a Integer property.
     *
     * @param properties Properties object which holds values for this
     *    property.
     * @param path Name by which this property is serialized to a properties
     *    file, for example "com.acme.trace.Verbosity".
     * @param defaultValue Default value, null if there is no default.
     */
    public IntegerProperty(Properties properties, String path,
            int defaultValue) {
        super(properties, path, Integer.toString(defaultValue));
    }

    /**
     * Retrieves the value of this integer property.
     * If the property has no value, returns the default value.
     * If there is no default value, returns 0.
     */
    public int get() {
        final String value = getInternal(null, false);
        if (value == null) {
            return 0;
        }
        return Integer.parseInt(value);
    }

    /**
     * Retrieves the value of this integer property.
     * If the property has no value, returns the default value.
     * If there is no default value, returns the given default value.
     */
    public int get(int defaultValue) {
        final String value = getInternal(null, false);
        if (value == null) {
            return defaultValue;
        }
        return Integer.parseInt(value);
    }
}

// End IntegerProperty.java
/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 SQLstream, Inc.
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2003-2007 John V. Sichi
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
package net.sf.farrago.plugin;

import java.sql.*;

import java.util.*;

import net.sf.farrago.resource.*;


/**
 * FarragoAbstractPluginBase is an abstract base for classes used to build
 * implementations of {@link FarragoPlugin}. Instances of this class are not
 * necessarily direct implementations of the FarragoPlugin interface itself;
 * they may be sub-components of the plugin.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoAbstractPluginBase
{
    //~ Static fields/initializers ---------------------------------------------

    /**
     * A zero-length array of DriverPropertyInfo.
     */
    public static final DriverPropertyInfo [] EMPTY_DRIVER_PROPERTIES =
        new DriverPropertyInfo[0];

    public static final String [] BOOLEAN_CHOICES_DEFAULT_FALSE =
    {
        "FALSE",
        "TRUE"
    };

    public static final String [] BOOLEAN_CHOICES_DEFAULT_TRUE =
    {
        "TRUE",
        "FALSE"
    };

    //~ Methods ----------------------------------------------------------------

    /**
     * Verifies that a property has been set, throwing an exception if has not.
     *
     * @param props properties to check
     * @param propName name of required property
     *
     * @exception EigenbaseException if property is not set
     */
    public static void requireProperty(
        Properties props,
        String propName)
    {
        if (props.getProperty(propName) == null) {
            throw FarragoResource.instance().PluginPropRequired.ex(propName);
        }
    }

    /**
     * Gets the value of a long integer property.
     *
     * @param props property set
     * @param propName name of property
     * @param defaultValue value to return if property is not set
     *
     * @return property value
     *
     * @exception EigenbaseException if property is set with non-integer value
     */
    public static long getLongProperty(
        Properties props,
        String propName,
        long defaultValue)
    {
        String s = props.getProperty(propName);
        if (s == null) {
            return defaultValue;
        } else {
            try {
                return Long.parseLong(s);
            } catch (NumberFormatException ex) {
                throw FarragoResource.instance().PluginInvalidLongProp.ex(
                    s,
                    propName);
            }
        }
    }

    /**
     * Gets the value of an integer property.
     *
     * @param props property set
     * @param propName name of property
     * @param defaultValue value to return if property is not set
     *
     * @return property value
     *
     * @exception EigenbaseException if property is set with non-integer value
     */
    public static int getIntProperty(
        Properties props,
        String propName,
        int defaultValue)
    {
        String s = props.getProperty(propName);
        if (s == null) {
            return defaultValue;
        } else {
            try {
                return Integer.parseInt(s);
            } catch (NumberFormatException ex) {
                throw FarragoResource.instance().PluginInvalidIntProp.ex(
                    s,
                    propName);
            }
        }
    }

    /**
     * Gets the value of an short property.
     *
     * @param props property set
     * @param propName name of property
     * @param defaultValue value to return if property is not set
     *
     * @return property value
     *
     * @exception EigenbaseException if property is set with non-short value
     */
    public static short getShortProperty(
        Properties props,
        String propName,
        short defaultValue)
    {
        String s = props.getProperty(propName);
        if (s == null) {
            return defaultValue;
        } else {
            try {
                return Short.parseShort(s);
            } catch (NumberFormatException ex) {
                throw FarragoResource.instance().PluginInvalidShortProp.ex(
                    s,
                    propName);
            }
        }
    }

    /**
     * Gets the value of a byte property.
     *
     * @param props property set
     * @param propName name of property
     * @param defaultValue value to return if property is not set
     *
     * @return property value
     *
     * @exception EigenbaseException if property is set with non-short value
     */
    public static byte getByteProperty(
        Properties props,
        String propName,
        byte defaultValue)
    {
        String s = props.getProperty(propName);
        if (s == null) {
            return defaultValue;
        } else {
            try {
                return Byte.parseByte(s);
            } catch (NumberFormatException ex) {
                throw FarragoResource.instance().PluginInvalidByteProp.ex(
                    s,
                    propName);
            }
        }
    }

    /**
     * Gets the value of an float property.
     *
     * @param props property set
     * @param propName name of property
     * @param defaultValue value to return if property is not set
     *
     * @return property value
     *
     * @exception EigenbaseException if property is set with non-short value
     */
    public static float getFloatProperty(
        Properties props,
        String propName,
        float defaultValue)
    {
        String s = props.getProperty(propName);
        if (s == null) {
            return defaultValue;
        } else {
            try {
                return Float.parseFloat(s);
            } catch (NumberFormatException ex) {
                throw FarragoResource.instance().PluginInvalidFloatProp.ex(
                    s,
                    propName);
            }
        }
    }

    /**
     * Gets the value of an double property.
     *
     * @param props property set
     * @param propName name of property
     * @param defaultValue value to return if property is not set
     *
     * @return property value
     *
     * @exception EigenbaseException if property is set with non-short value
     */
    public static double getDoubleProperty(
        Properties props,
        String propName,
        double defaultValue)
    {
        String s = props.getProperty(propName);
        if (s == null) {
            return defaultValue;
        } else {
            try {
                return Double.parseDouble(s);
            } catch (NumberFormatException ex) {
                throw FarragoResource.instance().PluginInvalidDoubleProp.ex(
                    s,
                    propName);
            }
        }
    }

    /**
     * Gets the value of a boolean property, or a default value if the property
     * does not exist. Returns <code>true</code> if the property exists, and its
     * value is <code>1</code>, <code>t</code>, <code>true</code> or <code>
     * yes</code>; the default value if it does not exist; <code>false</code>
     * otherwise.
     *
     * @param props property set
     * @param propName name of property
     * @param defaultValue value to return if property is not set
     *
     * @return property value
     */
    public static boolean getBooleanProperty(
        Properties props,
        String propName,
        boolean defaultValue)
    {
        String s = props.getProperty(propName);
        if (s == null) {
            return defaultValue;
        }
        return s.equalsIgnoreCase("1") || s.equalsIgnoreCase("t")
            || s.equalsIgnoreCase("true") || s.equalsIgnoreCase("yes")
            || s.equalsIgnoreCase("on");
    }
}

// End FarragoAbstractPluginBase.java

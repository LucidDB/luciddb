/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
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
package net.sf.farrago.plugin;

import java.sql.*;
import java.util.*;


/**
 * FarragoPluginInfoList is a helper class for building up the arrays of
 * DriverPropertyInfo returned by various getXXXPropertyInfo calls.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoPluginInfoList
{
    //~ Instance fields -------------------------------------------------------

    private Properties defaultProps;
    private List propertyInfoList;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates an empty info set.
     *
     * @param defaultProps Properties in which to look up default values
     */
    public FarragoPluginInfoList(Properties defaultProps)
    {
        this.defaultProps = defaultProps;
        propertyInfoList = new ArrayList();
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * Adds optional property information.
     *
     * @param propertyName name of the property
     *
     * @param defaultValue String representation of default value
     * to use if property name not present in defaultProps
     *
     * @param description localized description
     *
     * @return created info (the choices attribute can be set after return)
     */
    public DriverPropertyInfo addOptionalPropertyInfo(
        String propertyName,
        String defaultValue,
        String description)
    {
        String value = defaultProps.getProperty(propertyName);
        if (value == null) {
            value = defaultValue;
        }
        DriverPropertyInfo info = new DriverPropertyInfo(propertyName, value);
        propertyInfoList.add(info);
        info.description = description;
        return info;
    }

    /**
     * Adds required property information.
     *
     * @param propertyName name of the property
     *
     * @param defaultValue String representation of default value
     * to use if property name not present in defaultProps
     *
     * @param description localized description
     *
     * @return created info (the choices attribute can be set after return)
     */
    public DriverPropertyInfo addRequiredPropertyInfo(
        String propertyName,
        String defaultValue,
        String description)
    {
        DriverPropertyInfo info =
            addOptionalPropertyInfo(propertyName, defaultValue, description);
        info.required = true;
        return info;
    }

    /**
     * Converts the list built up with addXXXPropertyInfo to an array
     * suitable for return from a getXXXPropertyInfo call.
     *
     * @return the converted array
     */
    public DriverPropertyInfo [] toArray()
    {
        return (DriverPropertyInfo []) propertyInfoList.toArray(FarragoAbstractPluginBase.EMPTY_DRIVER_PROPERTIES);
    }
}


// End FarragoPluginInfoList.java

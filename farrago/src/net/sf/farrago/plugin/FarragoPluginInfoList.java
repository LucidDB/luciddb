/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
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
    //~ Instance fields --------------------------------------------------------

    private Properties defaultProps;
    private List<DriverPropertyInfo> propertyInfoList;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates an empty info set.
     *
     * @param defaultProps Properties in which to look up default values
     */
    public FarragoPluginInfoList(Properties defaultProps)
    {
        this.defaultProps = defaultProps;
        propertyInfoList = new ArrayList<DriverPropertyInfo>();
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Adds optional property information.
     *
     * @param propertyName name of the property
     * @param defaultValue String representation of default value to use if
     * property name not present in defaultProps
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
     * @param defaultValue String representation of default value to use if
     * property name not present in defaultProps
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
     * Converts the list built up with addXXXPropertyInfo to an array suitable
     * for return from a getXXXPropertyInfo call.
     *
     * @return the converted array
     */
    public DriverPropertyInfo [] toArray()
    {
        return propertyInfoList.toArray(
            FarragoAbstractPluginBase.EMPTY_DRIVER_PROPERTIES);
    }
}

// End FarragoPluginInfoList.java

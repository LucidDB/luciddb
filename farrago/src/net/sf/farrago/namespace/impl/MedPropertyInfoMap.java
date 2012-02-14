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
package net.sf.farrago.namespace.impl;

import java.sql.*;

import java.util.*;


/**
 * MedPropertyInfoMap collects information to be returned via DriverPropertyInfo
 * calls.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class MedPropertyInfoMap
    extends MedAbstractBase
{
    //~ Instance fields --------------------------------------------------------

    private final ResourceBundle resourceBundle;
    private final String resourcePrefix;
    private final Properties proposedProps;
    private final Map<String, DriverPropertyInfo> map;

    //~ Constructors -----------------------------------------------------------

    public MedPropertyInfoMap(
        ResourceBundle resourceBundle,
        String resourcePrefix,
        Properties proposedProps)
    {
        this.resourceBundle = resourceBundle;
        this.resourcePrefix = resourcePrefix;
        this.proposedProps = proposedProps;
        this.map = new LinkedHashMap<String, DriverPropertyInfo>();
    }

    //~ Methods ----------------------------------------------------------------

    public void addPropInfo(
        String propName)
    {
        addPropInfo(propName, false, null);
    }

    public void addPropInfo(
        String propName,
        boolean required)
    {
        addPropInfo(propName, required, null);
    }

    public void addPropInfo(
        String propName,
        boolean required,
        String [] choices)
    {
        DriverPropertyInfo info = new DriverPropertyInfo(propName, null);
        String key = resourcePrefix + "_" + propName + "_Description";
        try {
            info.description = resourceBundle.getString(key);
        } catch (MissingResourceException ex) {
            // NOTE jvs 18-June-2006:  This is supposed to serve
            // as a not-so-gentle reminder to localize whenever it shows
            // up in a UI.
            info.description = "UNLOCALIZED_" + propName + "_DESCRIPTION";
        }
        info.required = required;
        info.choices = choices;
        info.value = proposedProps.getProperty(propName);
        if (info.value == null) {
            if ((choices != null) && (choices.length > 0)) {
                info.value = choices[0];
            }
        }
        map.put(propName, info);
    }

    public DriverPropertyInfo [] toArray()
    {
        return (DriverPropertyInfo []) map.values().toArray(
            EMPTY_DRIVER_PROPERTIES);
    }
}

// End MedPropertyInfoMap.java

/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2007 The Eigenbase Project
// Copyright (C) 2006-2007 Disruptive Tech
// Copyright (C) 2006-2007 LucidEra, Inc.
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

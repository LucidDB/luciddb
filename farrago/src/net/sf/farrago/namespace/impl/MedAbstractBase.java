/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
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

package net.sf.farrago.namespace.impl;

import net.sf.farrago.resource.*;

import java.util.*;
import java.sql.*;

/**
 * MedAbstractBase is an abstract base for classes used to build data wrappers.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class MedAbstractBase
{
    /**
     * A zero-length array of DriverPropertyInfo.
     */
    public static final DriverPropertyInfo [] EMPTY_DRIVER_PROPERTIES
        = new DriverPropertyInfo[0];

    /**
     * Gets the value of a long integer property.
     *
     * @param props property set
     *
     * @param propName name of property
     *
     * @param defaultValue value to return if property is not set
     *
     * @return property value
     *
     * @exception SQLException if property is set with non-integer value
     */
    public static long getLongProperty(
        Properties props,String propName,long defaultValue)
        throws SQLException
    {
        String s = props.getProperty(propName);
        if (s == null) {
            return defaultValue;
        } else {
            try {
                return Long.parseLong(s);
            } catch (NumberFormatException ex) {
                throw FarragoResource.instance().newDataWrapperInvalidIntProp(
                    s,propName);
            }
        }
    }
    
    /**
     * Gets the value of an integer property.
     *
     * @param props property set
     *
     * @param propName name of property
     *
     * @param defaultValue value to return if property is not set
     *
     * @return property value
     *
     * @exception SQLException if property is set with non-integer value
     */
    public static int getIntProperty(
        Properties props,String propName,int defaultValue)
        throws SQLException
    {
        String s = props.getProperty(propName);
        if (s == null) {
            return defaultValue;
        } else {
            try {
                return Integer.parseInt(s);
            } catch (NumberFormatException ex) {
                throw FarragoResource.instance().newDataWrapperInvalidIntProp(
                    s,propName);
            }
        }
    }
}

// End MedAbstractBase.java

/*
// Farrago is a relational database management system.
// Copyright (C) 2004-2004 John V. Sichi.
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
package net.sf.farrago.jdbc;

import java.sql.DriverPropertyInfo;
import java.util.Locale;
import java.util.Properties;

/**
 * Description of a SQL/MED data wrapper.
 *
 * @author Julian Hyde
 * @version $Id$
 */
public interface FarragoMedDataWrapperInfo
{
    /**
     * Obtains information about the properties applicable to server
     * initialization (the props parameter to the newServer method).
     *
     * @param locale Locale for formatting property info
     *
     * @param wrapperProps proposed list of property name/value
     * pairs which will be sent to
     * {@link net.sf.farrago.namespace.FarragoMedDataWrapper#initialize}
     *
     * @param serverProps proposed list of property name/value
     * pairs which will be sent to
     * {@link net.sf.farrago.namespace.FarragoMedDataWrapper#newServer}
     *
     * @return 0 or more property info descriptors
     */
    public DriverPropertyInfo [] getServerPropertyInfo(
        Locale locale,
        Properties wrapperProps,
        Properties serverProps);

    /**
     * Obtains information about the properties applicable to column set
     * initialization (the tableProps parameter to the newColumnSet method).
     *
     * @param locale Locale for formatting property info
     *
     * @param wrapperProps proposed list of property name/value
     * pairs which will be sent to
     * {@link net.sf.farrago.namespace.FarragoMedDataWrapper#initialize}
     *
     * @param serverProps proposed list of property name/value
     * pairs which will be sent to
     * {@link net.sf.farrago.namespace.FarragoMedDataWrapper#newServer}
     *
     * @param tableProps proposed list of property name/value pairs which will
     * be sent to the tableProps parameter of
     * {@link net.sf.farrago.namespace.FarragoMedDataServer#newColumnSet}
     *
     * @return 0 or more property info descriptors
     */
    public DriverPropertyInfo [] getColumnSetPropertyInfo(
        Locale locale,
        Properties wrapperProps,
        Properties serverProps,
        Properties tableProps);

    /**
     * Obtains information about the properties applicable to individual column
     * initialization (the columnPropMap parameter to the
     * {@link net.sf.farrago.namespace.FarragoMedDataServer#newColumnSet}
     * method).
     *
     * @param locale Locale for formatting property info
     *
     * @param wrapperProps proposed list of property name/value
     * pairs which will be sent to
     * {@link net.sf.farrago.namespace.FarragoMedDataWrapper#initialize}
     *
     * @param serverProps proposed list of property name/value
     * pairs which will be sent to FarragoMedDataWrapper.newServer()
     * {@link net.sf.farrago.namespace.FarragoMedDataWrapper#newServer}
     *
     * @param tableProps proposed list of property name/value
     * pairs which will be sent as the tableProps parameter of
     * {@link net.sf.farrago.namespace.FarragoMedDataServer#newColumnSet}
     *
     * @param columnProps proposed list of property name/value pairs which will
     * be sent as an entry in the columnPropMap parameter of
     * {@link net.sf.farrago.namespace.FarragoMedDataServer#newColumnSet}
     *
     * @return 0 or more property info descriptors
     */
    public DriverPropertyInfo [] getColumnPropertyInfo(
        Locale locale,
        Properties wrapperProps,
        Properties serverProps,
        Properties tableProps,
        Properties columnProps);

    /**
     * Determines whether this data wrapper accesses foreign data, or
     * manages local data.
     *
     * @return true for foreign data; false for local data
     */
    public boolean isForeign();
}

// End FarragoMedDataWrapperInfo.java

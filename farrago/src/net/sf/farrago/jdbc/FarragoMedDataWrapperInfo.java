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
package net.sf.farrago.jdbc;

import java.sql.*;

import java.util.*;


/**
 * Description of a SQL/MED data wrapper.
 *
 * @author Julian Hyde
 * @version $Id$
 */
public interface FarragoMedDataWrapperInfo
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Obtains information about the properties applicable to plugin
     * initialization.
     *
     * @param locale Locale for formatting property info
     * @param wrapperProps proposed list of property name/value pairs which will
     * be sent to {@link
     * net.sf.farrago.namespace.FarragoMedDataWrapper#initialize}
     *
     * @return 0 or more property info descriptors
     */
    public DriverPropertyInfo [] getPluginPropertyInfo(
        Locale locale,
        Properties wrapperProps);

    /**
     * Obtains information about the properties applicable to server
     * initialization (the props parameter to the newServer method).
     *
     * @param locale Locale for formatting property info
     * @param wrapperProps proposed list of property name/value pairs which will
     * be sent to {@link
     * net.sf.farrago.namespace.FarragoMedDataWrapper#initialize}
     * @param serverProps proposed list of property name/value pairs which will
     * be sent to {@link
     * net.sf.farrago.namespace.FarragoMedDataWrapper#newServer}
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
     * @param wrapperProps proposed list of property name/value pairs which will
     * be sent to {@link
     * net.sf.farrago.namespace.FarragoMedDataWrapper#initialize}
     * @param serverProps proposed list of property name/value pairs which will
     * be sent to {@link
     * net.sf.farrago.namespace.FarragoMedDataWrapper#newServer}
     * @param tableProps proposed list of property name/value pairs which will
     * be sent to the tableProps parameter of {@link
     * net.sf.farrago.namespace.FarragoMedDataServer#newColumnSet}
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
     * initialization (the columnPropMap parameter to the {@link
     * net.sf.farrago.namespace.FarragoMedDataServer#newColumnSet} method).
     *
     * @param locale Locale for formatting property info
     * @param wrapperProps proposed list of property name/value pairs which will
     * be sent to {@link
     * net.sf.farrago.namespace.FarragoMedDataWrapper#initialize}
     * @param serverProps proposed list of property name/value pairs which will
     * be sent to FarragoMedDataWrapper.newServer() {@link
     * net.sf.farrago.namespace.FarragoMedDataWrapper#newServer}
     * @param tableProps proposed list of property name/value pairs which will
     * be sent as the tableProps parameter of {@link
     * net.sf.farrago.namespace.FarragoMedDataServer#newColumnSet}
     * @param columnProps proposed list of property name/value pairs which will
     * be sent as an entry in the columnPropMap parameter of {@link
     * net.sf.farrago.namespace.FarragoMedDataServer#newColumnSet}
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
     * Determines whether this data wrapper accesses foreign data, or manages
     * local data.
     *
     * @return true for foreign data; false for local data
     */
    public boolean isForeign();
}

// End FarragoMedDataWrapperInfo.java

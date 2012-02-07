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

import net.sf.farrago.catalog.*;
import net.sf.farrago.util.*;


/**
 * FarragoPlugin defines an abstract plugin interface. Some JDBC infrastructure
 * is borrowed ({@link java.sql.SQLException} and {@link
 * java.sql.DriverPropertyInfo}). The property info calls are designed to work
 * in the same iterative fashion as {@link java.sql.Driver#getPropertyInfo}.
 *
 * <p>Implementations of FarragoPlugin must provide a public default constructor
 * in order to be loaded via DDL statements. FarragoPlugin extends {@link
 * FarragoAllocation}; when closeAllocation is called, all resources acquired by
 * the plugin should be released.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface FarragoPlugin
    extends FarragoAllocation
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Obtains a suggested name for this plugin in the SQL catalog.
     *
     * @return suggested name
     */
    public String getSuggestedName();

    /**
     * Obtains a description of this plugin.
     *
     * @param locale Locale for formatting description
     *
     * @return localized description
     */
    public String getDescription(Locale locale);

    /**
     * Obtains information about the properties applicable to plugin
     * initialization (the props parameter to the initialize method).
     *
     * @param locale Locale for formatting property info
     * @param props proposed list of property name/value pairs which will be
     * sent to initialize()
     *
     * @return 0 or more property info descriptors
     */
    public DriverPropertyInfo [] getPluginPropertyInfo(
        Locale locale,
        Properties props);

    /**
     * Initializes this plugin with a given set of properties. This is called
     * after an uninitialized instance has been created via Class.forName. As
     * much validation as possible should be performed.
     *
     * @param repos FarragoRepos which can be used for metadata access
     * @param props plugin properties
     *
     * @exception SQLException if plugin initialization is unsuccessful
     */
    public void initialize(
        FarragoRepos repos,
        Properties props)
        throws SQLException;

    /**
     * set the library name used to initialize this plugin
     *
     * @param libraryName library name
     */
    public void setLibraryName(String libraryName);

    /**
     * return the library name used to initialize this plugin
     *
     * @return library name
     */
    public String getLibraryName();

    /**
     * return the options with which this plugin was initialized
     *
     * @return plugin properties
     */
    public Properties getProperties();
}

// End FarragoPlugin.java

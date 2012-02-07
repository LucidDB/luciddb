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

import net.sf.farrago.catalog.*;
import net.sf.farrago.namespace.*;


/**
 * MedAbstractDataWrapper is an abstract base class for implementations of the
 * {@link FarragoMedDataWrapper} interface.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class MedAbstractDataWrapper
    extends MedAbstractBase
    implements FarragoMedDataWrapper
{
    //~ Instance fields --------------------------------------------------------

    private FarragoRepos repos;
    private Properties props;
    private String libraryName;

    //~ Constructors -----------------------------------------------------------

    protected MedAbstractDataWrapper()
    {
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * @return the repos with which this wrapper was initialized
     */
    public FarragoRepos getRepos()
    {
        return repos;
    }

    /**
     * @return the options specified by CREATE FOREIGN DATA WRAPPER
     */
    public Properties getProperties()
    {
        return props;
    }

    /**
     * @return the library name used to initialize this wrapper
     */
    public String getLibraryName()
    {
        return libraryName;
    }

    /**
     * @param libraryName library name used to initialize this wrapper
     */
    public void setLibraryName(String libraryName)
    {
        this.libraryName = libraryName;
    }

    // implement FarragoMedDataWrapper
    public DriverPropertyInfo [] getPluginPropertyInfo(
        Locale locale,
        Properties props)
    {
        return EMPTY_DRIVER_PROPERTIES;
    }

    // implement FarragoMedDataWrapper
    public DriverPropertyInfo [] getServerPropertyInfo(
        Locale locale,
        Properties wrapperProps,
        Properties serverProps)
    {
        return EMPTY_DRIVER_PROPERTIES;
    }

    // implement FarragoMedDataWrapper
    public DriverPropertyInfo [] getColumnSetPropertyInfo(
        Locale locale,
        Properties wrapperProps,
        Properties serverProps,
        Properties tableProps)
    {
        return EMPTY_DRIVER_PROPERTIES;
    }

    // implement FarragoMedDataWrapper
    public DriverPropertyInfo [] getColumnPropertyInfo(
        Locale locale,
        Properties wrapperProps,
        Properties serverProps,
        Properties tableProps,
        Properties columnProps)
    {
        return EMPTY_DRIVER_PROPERTIES;
    }

    // implement FarragoMedDataWrapper
    public void initialize(
        FarragoRepos repos,
        Properties props)
        throws SQLException
    {
        this.repos = repos;
        this.props = props;
    }

    // implement FarragoAllocation
    public void closeAllocation()
    {
    }

    // implement FarragoMedDataWrapper
    public boolean isForeign()
    {
        return true;
    }

    // implement FarragoMedDataWrapper
    public boolean supportsServerSharing()
    {
        return false;
    }
}

// End MedAbstractDataWrapper.java

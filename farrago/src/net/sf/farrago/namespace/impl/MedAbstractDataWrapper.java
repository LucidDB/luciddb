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

import java.sql.*;
import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.namespace.*;


/**
 * MedAbstractDataWrapper is an abstract base class for
 * implementations of the {@link FarragoMedDataWrapper} interface.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class MedAbstractDataWrapper extends MedAbstractBase
    implements FarragoMedDataWrapper
{
    //~ Instance fields -------------------------------------------------------

    private FarragoRepos repos;
    private Properties props;

    //~ Constructors ----------------------------------------------------------

    protected MedAbstractDataWrapper()
    {
    }

    //~ Methods ---------------------------------------------------------------

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
}


// End MedAbstractDataWrapper.java

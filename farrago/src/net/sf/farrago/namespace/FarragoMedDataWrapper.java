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

package net.sf.farrago.namespace;

import net.sf.saffron.core.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.type.*;
import net.sf.farrago.util.*;

import java.sql.*;
import java.util.*;

/**
 * FarragoMedDataWrapper defines an interface for accessing foreign or local
 * data.  It is a non-standard replacement for the standard SQL/MED internal
 * interface.  Some JDBC infrastructure is borrowed ({@link
 * java.sql.SQLException} and {@link java.sql.DriverPropertyInfo}).  The
 * property info calls are designed to work in the same iterative fashion as
 * {@link java.sql.Driver#getPropertyInfo}.
 *
 *<p>
 *
 * Implementations of FarragoMedDataWrapper must provide a public default
 * constructor in order to be loaded via the CREATE {FOREIGN|LOCAL} DATA
 * WRAPPER statement.  FarragoMedDataWrapper extends FarragoAllocation; when
 * closeAllocation is called, all resources (such as connections) used to
 * access the data should be released.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface FarragoMedDataWrapper extends FarragoAllocation
{
    /**
     * Obtains a suggested name for this wrapper in the SQL catalog.
     *
     * @return suggested name
     */
    public String getSuggestedName();
    
    /**
     * Obtains a description of this wrapper.
     *
     * @param locale Locale for formatting description
     *
     * @return localized description
     */
    public String getDescription(Locale locale);
    
    /**
     * Obtains information about the properties applicable to wrapper
     * initialization (the props parameter to the initialize method).
     *
     * @param locale Locale for formatting property info
     *
     * @param props proposed list of property name/value
     * pairs which will be sent to initialize()
     *
     * @return 0 or more property info descriptors
     */
    public DriverPropertyInfo [] getWrapperPropertyInfo(
        Locale locale,
        Properties props);

    /**
     * Obtains information about the properties applicable to server
     * initialization (the props parameter to the newServer method).
     *
     * @param locale Locale for formatting property info
     *
     * @param wrapperProps proposed list of property name/value
     * pairs which will be sent to FarragoMedDataWrapper.initialize()
     *
     * @param serverProps proposed list of property name/value
     * pairs which will be sent to FarragoMedDataWrapper.newServer()
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
     * pairs which will be sent to FarragoMedDataWrapper.initialize()
     *
     * @param serverProps proposed list of property name/value
     * pairs which will be sent to FarragoMedDataWrapper.newServer()
     *
     * @param tableProps proposed list of property name/value pairs which will
     * be sent to the tableProps parameter of
     * FarragoMedDataServer.newColumnSet()
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
     * initialization (the columnPropMap parameter to the newColumnSet method).
     *
     * @param locale Locale for formatting property info
     *
     * @param wrapperProps proposed list of property name/value
     * pairs which will be sent to FarragoMedDataWrapper.initialize()
     *
     * @param serverProps proposed list of property name/value
     * pairs which will be sent to FarragoMedDataWrapper.newServer()
     *
     * @param tableProps proposed list of property name/value
     * pairs which will be sent as the tableProps parameter of
     * FarragoMedDataServer.newColumnSet()
     *
     * @param columnProps proposed list of property name/value pairs which will
     * be sent as an entry in the columnPropMap parameter of
     * FarragoMedDataServer.newColumnSet()
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
     * Initializes this wrapper with a given set of properties.  This is called
     * after an uninitialized instance has been created via Class.forName.  As
     * much validation as possible should be performed, including establishing
     * connections if appropriate.
     *
     * @param catalog FarragoCatalog which can be used for metadata access
     *
     * @param props wrapper properties
     *
     * @exception SQLException if wrapper initialization is unsuccessful
     */
    public void initialize(
        FarragoCatalog catalog,
        Properties props)
        throws SQLException;

    /**
     * Creates an instance of this wrapper for a particular server.
     * This supports the SQL/MED CREATE SERVER statement.  The
     * TYPE and VERSION attributes are rolled in with the other
     * properties.  As much validation as possible should
     * be performed, including establishing connections
     * if appropriate.
     *
     *<p>
     *
     * If this wrapper returns false from the isForeign method, then
     * returned server instances must implement the FarragoMedLocalDataServer
     * interface.
     *
     * @param serverMofId MOFID of server definition in repository;
     * this can be used for accessing the server definition from
     * generated code
     *
     * @param props server properties
     *
     * @return new server instance
     *
     * @exception SQLException if server connection is unsuccessful
     */
    public FarragoMedDataServer newServer(
        String serverMofId,
        Properties props)
        throws SQLException;

    /**
     * Determines whether this data wrapper accesses foreign data, or
     * manages local data.
     *
     * @return true for foreign data; false for local data
     */
    public boolean isForeign();
}

// End FarragoMedDataWrapper.java

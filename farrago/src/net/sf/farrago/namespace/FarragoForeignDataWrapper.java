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

import net.sf.saffron.core.*;

import java.sql.*;
import java.util.*;

/**
 * FarragoForeignDataWrapper defines an interface for accessing external data.
 * It is a non-standard replacement for the standard SQL/MED internal
 * interface, and doubles as both a generic wrapper instance and a
 * server-specific instance.  Some JDBC infrastructure is borrowed
 * (SQLException and DriverPropertyInfo).
 *
 *<p>
 *
 * Implementations of FarragoForeignDataWrapper must provide a public
 * default constructor in order to be loaded via the CREATE FOREIGN DATA
 * WRAPPER statement.  FarragoForeignDataWrapper extends FarragoAllocation;
 * when closeAllocation is called, all resources (such as connections)
 * used to access the external data should be released.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface FarragoForeignDataWrapper extends FarragoAllocation
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
     * initialization (props parameter to the initialize method).
     *
     * @param locale Locale for formatting property info
     *
     * @return 0 or more property info descriptors
     */
    public DriverPropertyInfo [] getWrapperPropertyInfo(Locale locale);

    /**
     * Obtains information about the properties applicable to server
     * initialization (props parameter to the newServer method).
     *
     * @param locale Locale for formatting property info
     *
     * @return 0 or more property info descriptors
     */
    public DriverPropertyInfo [] getServerPropertyInfo(Locale locale);

    /**
     * Obtains information about the properties applicable to column set
     * initialization (tableProps parameter to the newColumnSet method).
     *
     * @param locale Locale for formatting property info
     *
     * @return 0 or more property info descriptors
     */
    public DriverPropertyInfo [] getColumnSetPropertyInfo(Locale locale);

    /**
     * Obtains information about the properties applicable to individual column
     * initialization (columnPropMap parameter to the newColumnSet method).
     *
     * @param locale Locale for formatting property info
     *
     * @return 0 or more property info descriptors
     */
    public DriverPropertyInfo [] getColumnPropertyInfo(Locale locale);

    /**
     * Initializes this wrapper with a given set of properties.  This
     * supports the SQL/MED CREATE FOREIGN DATA WRAPPER statement,
     * and is called after an uninitialized instance has been created
     * via Class.forName.  As much validation as possible should
     * be performed, including establishing connections
     * if appropriate.
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
     * @param serverMofId MOFID of server definition in repository;
     * this can be used for accessing the server definition from
     * generated code
     *
     * @param props server properties
     *
     * @exception SQLException if server connection is unsuccessful
     */
    public FarragoForeignDataWrapper newServer(
        String serverMofId,
        Properties props)
        throws SQLException;

    /**
     * Gets a FarragoNameDirectory corresponding to this wrapper.
     *
     * @return directory, or null if this wrapper does not have
     * the required metadata capability
     *
     * @exception SQLException if directory access is unsuccessful
     * (but not if directory access is unsupported)
     */
    public FarragoNameDirectory getNameDirectory()
        throws SQLException;

    /**
     * Creates an instance of a FarragoNamedColumnSet corresponding to row data
     * which is identified by properties rather than by name.  This supports
     * the SQL/MED CREATE FOREIGN TABLE statement.  As much validation as
     * possible should be performed, including accessing representative data.
     *
     * @param qualifiedName the qualified name to assign to the column set
     * within Farrago; this should NOT be used for finding the actual data,
     * since it can be set arbitrarily by the caller; instead, it
     * should be used to implement the SaffronTable.getQualifiedName() method,
     * and can be useful for correlation during debugging
     *
     * @param tableProps properties to use for data location and access
     *
     * @param typeFactory FarragoTypeFactory to use
     * for defining types
     *
     * @param rowType type to impose on the rows of this column set
     * (including column names and types), or null to infer row type;
     * if this is non-null, it must be saved for use by the
     * getRowType() returned FarragoNamedColumnSet
     *
     * @param columnPropMap map from column name to column-specific
     * Properties; this is optional and may only be specified when
     * rowType is also specified (the field names in rowType are
     * used as the keys for columnPropMap)
     *
     * @return new FarragoNamedColumnSet
     *
     * @exception SQLException if data access is unsuccessful
     */
    public FarragoNamedColumnSet newColumnSet(
        String [] qualifiedName,
        Properties tableProps,
        FarragoTypeFactory typeFactory,
        SaffronType rowType,
        Map columnPropMap)
        throws SQLException;

    /**
     * Gets an object needed for runtime support.  Typically, this will be
     * called from code generated by this wrapper.  The meaning of this is
     * entirely dependent on the wrapper implementation.  If the returned
     * object implements FarragoAllocation, its closeAllocation() method
     * will be called automatically as soon as it is no longer needed.
     *
     * @param param parameter supplied at runtime
     *
     * @return support object
     */
    public Object getRuntimeSupport(Object param) throws SQLException;

    /**
     * Gives this wrapper a chance to register any special optimization rules.
     * This method may be called more than once, each time with a different
     * planner instance.
     *
     * @param planner the planner in which the rules should
     * be registered
     */
    public void registerRules(SaffronPlanner planner);
}

// End FarragoForeignDataWrapper.java

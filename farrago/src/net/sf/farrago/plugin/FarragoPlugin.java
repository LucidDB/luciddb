/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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

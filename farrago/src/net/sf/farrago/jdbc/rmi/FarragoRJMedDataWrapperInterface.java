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
package net.sf.farrago.jdbc.rmi;

import net.sf.farrago.catalog.FarragoRepos;
import net.sf.farrago.namespace.FarragoMedDataServer;

import java.rmi.RemoteException;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Locale;
import java.util.Properties;

/**
 * RMI server interface corresponding to
 * {@link net.sf.farrago.namespace.FarragoMedDataWrapper}.
 *
 * @author Tim Leung
 * @version $Id$
 */
public interface FarragoRJMedDataWrapperInterface extends java.rmi.Remote {
    /**
     * @see net.sf.farrago.namespace.FarragoMedDataWrapper#getServerPropertyInfo
     */
    DriverPropertyInfo [] getServerPropertyInfo(
        Locale locale,
        Properties wrapperProps,
        Properties serverProps)
        throws RemoteException;

    /**
     * @see net.sf.farrago.namespace.FarragoMedDataWrapper#getColumnSetPropertyInfo
     */
    DriverPropertyInfo [] getColumnSetPropertyInfo(
        Locale locale,
        Properties wrapperProps,
        Properties serverProps,
        Properties tableProps)
        throws RemoteException;

    /**
     * @see net.sf.farrago.namespace.FarragoMedDataWrapper#getColumnPropertyInfo
     */
    DriverPropertyInfo [] getColumnPropertyInfo(
        Locale locale,
        Properties wrapperProps,
        Properties serverProps,
        Properties tableProps,
        Properties columnProps)
        throws RemoteException;

    /**
     * @see net.sf.farrago.namespace.FarragoMedDataWrapper#newServer
     */
    FarragoMedDataServer newServer(
        String serverMofId,
        Properties props)
        throws RemoteException, SQLException;

    /**
     * @see net.sf.farrago.namespace.FarragoMedDataWrapper#isForeign
     */
    boolean isForeign()
        throws RemoteException;

    /**
     * @see net.sf.farrago.namespace.FarragoMedDataWrapper#initialize
     */
    void initialize(
        FarragoRepos repos,
        Properties props)
        throws RemoteException, SQLException;

    /**
     * @see net.sf.farrago.namespace.FarragoMedDataWrapper#getDescription
     */
    String getDescription(Locale locale) throws RemoteException;

    /**
     * @see net.sf.farrago.namespace.FarragoMedDataWrapper#getSuggestedName
     */
    String getSuggestedName() throws RemoteException;

    /**
     * @see net.sf.farrago.namespace.FarragoMedDataWrapper#getPluginPropertyInfo
     */
    DriverPropertyInfo[] getPluginPropertyInfo(
        Locale locale,
        Properties props) throws RemoteException;

    /**
     * @see net.sf.farrago.namespace.FarragoMedDataWrapper#closeAllocation
     */
    void closeAllocation() throws RemoteException;
}

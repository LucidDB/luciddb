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
package net.sf.farrago.server;

import net.sf.farrago.catalog.FarragoRepos;
import net.sf.farrago.jdbc.rmi.FarragoRJMedDataWrapperInterface;
import net.sf.farrago.jdbc.FarragoConnection;
import net.sf.farrago.namespace.FarragoMedDataServer;
import net.sf.farrago.namespace.FarragoMedDataWrapper;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.server.Unreferenced;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Locale;
import java.util.Properties;

/**
 * RMI server-side implementation of {@link FarragoMedDataWrapper}.
 *
 * <p>This object is constructed with a factory for creating a data wrapper.
 * Each method grabs a data wrapper from the factory, and releases it at the
 * end of the method. This class is therefore stateless: data wrappers are
 * never held between calls.
 *
 * @author Tim Leung
 * @version $Id$
 */
class FarragoRJMedDataWrapperServer 
    extends UnicastRemoteObject
    implements FarragoRJMedDataWrapperInterface, Unreferenced
{
    private final FarragoConnection farragoConnection;
    private final String mofId;
    private final String libraryName;
    private final Properties options;

    FarragoRJMedDataWrapperServer(
        FarragoConnection farragoConnection,
        String mofId,
        String libraryName,
        Properties options)
        throws RemoteException
    {
        super(FarragoRJJdbcServer.rmiJdbcListenerPort,
            FarragoRJJdbcServer.rmiClientSocketFactory,
            FarragoRJJdbcServer.rmiServerSocketFactory);
        this.farragoConnection = farragoConnection;
        this.mofId = mofId;
        this.libraryName = libraryName;
        this.options = (Properties) options.clone();
    }

    public void unreferenced()
    {
        //cache_.unloadWrapper(mofId_, libraryName_, options_);
    }

    public DriverPropertyInfo [] getServerPropertyInfo(
        Locale locale,
        Properties wrapperProps,
        Properties serverProps)
        throws RemoteException
    {
        final FarragoMedDataWrapper dataWrapper = createWrapper();
        try {
            return dataWrapper.getServerPropertyInfo(locale, wrapperProps,
                serverProps);
        } finally {
            dataWrapper.closeAllocation();
        }
    } 

    public DriverPropertyInfo [] getColumnSetPropertyInfo(
        Locale locale,
        Properties wrapperProps,
        Properties serverProps,
        Properties tableProps)
        throws RemoteException
    {
        final FarragoMedDataWrapper dataWrapper = createWrapper();
        try {
            return dataWrapper.getColumnSetPropertyInfo(locale, wrapperProps,
                serverProps, tableProps);
        } finally {
            dataWrapper.closeAllocation();
        }
    }

    public DriverPropertyInfo [] getColumnPropertyInfo(
        Locale locale,
        Properties wrapperProps,
        Properties serverProps,
        Properties tableProps,
        Properties columnProps)
        throws RemoteException
    {
        final FarragoMedDataWrapper dataWrapper = createWrapper();
        try {
            return dataWrapper.getColumnPropertyInfo(locale, wrapperProps,
                serverProps, tableProps, columnProps);
        } finally {
            dataWrapper.closeAllocation();
        }
    }

    public FarragoMedDataServer newServer(
        String serverMofId,
        Properties props)
        throws RemoteException, SQLException
    {
        final FarragoMedDataWrapper dataWrapper = createWrapper();
        try {
            return dataWrapper.newServer(serverMofId, props);
        } finally {
            dataWrapper.closeAllocation();
        }
    }

    public boolean isForeign() throws RemoteException
    {
        final FarragoMedDataWrapper dataWrapper = createWrapper();
        try {
            return dataWrapper.isForeign();
        } finally {
            dataWrapper.closeAllocation();
        }
    }

    public void initialize(FarragoRepos repos, Properties props)
        throws RemoteException, SQLException
    {
        final FarragoMedDataWrapper dataWrapper = createWrapper();
        try {
            dataWrapper.initialize(repos, props);
        } finally {
            dataWrapper.closeAllocation();
        }
    }

    public String getDescription(Locale locale)
        throws RemoteException
    {
        final FarragoMedDataWrapper dataWrapper = createWrapper();
        try {
            return dataWrapper.getDescription(locale);
        } finally {
            dataWrapper.closeAllocation();
        }
    }

    public String getSuggestedName()
        throws RemoteException
    {
        final FarragoMedDataWrapper dataWrapper = createWrapper();
        try {
            return dataWrapper.getSuggestedName();
        } finally {
            dataWrapper.closeAllocation();
        }
    }

    public DriverPropertyInfo[] getPluginPropertyInfo(
        Locale locale,
        Properties props)
        throws RemoteException
    {
        final FarragoMedDataWrapper dataWrapper = createWrapper();
        try {
            return dataWrapper.getPluginPropertyInfo(locale, props);
        } finally {
            dataWrapper.closeAllocation();
        }
    }

    private FarragoMedDataWrapper createWrapper() throws RemoteException {
        try {
            return farragoConnection.getWrapper(mofId, libraryName, options);
        } catch (SQLException e) {
            throw new RemoteException("", e);
        }
    }

    public void closeAllocation()
        throws RemoteException
    {
        // nothing to do -- because we release the dataWrapper after every call
    }
}

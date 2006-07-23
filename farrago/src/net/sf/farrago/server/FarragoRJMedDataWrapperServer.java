/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2004-2005 John V. Sichi
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
package net.sf.farrago.server;

import java.rmi.*;
import java.rmi.server.*;

import java.sql.*;

import java.util.*;

import net.sf.farrago.jdbc.*;
import net.sf.farrago.jdbc.rmi.*;


/**
 * RMI server-side implementation of {@link FarragoMedDataWrapper}.
 *
 * <p>This object is constructed with a factory for creating a data wrapper.
 * Each method grabs a data wrapper from the factory, and releases it at the end
 * of the method. This class is therefore stateless: data wrappers are never
 * held between calls.
 *
 * @author Tim Leung
 * @version $Id$
 */
class FarragoRJMedDataWrapperServer
    extends UnicastRemoteObject
    implements FarragoRJMedDataWrapperInterface,
        Unreferenced
{

    //~ Instance fields --------------------------------------------------------

    private final FarragoConnection farragoConnection;
    private final String mofId;
    private final String libraryName;
    private final Properties options;

    //~ Constructors -----------------------------------------------------------

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

    //~ Methods ----------------------------------------------------------------

    public void unreferenced()
    {
        //cache_.unloadWrapper(mofId_, libraryName_, options_);
    }

    public FarragoRJDriverPropertyInfo [] getPluginPropertyInfo(
        Locale locale,
        Properties wrapperProps)
        throws RemoteException
    {
        return
            makeSerializable(
                getWrapper().getPluginPropertyInfo(
                    locale,
                    wrapperProps));
    }

    public FarragoRJDriverPropertyInfo [] getServerPropertyInfo(
        Locale locale,
        Properties wrapperProps,
        Properties serverProps)
        throws RemoteException
    {
        return
            makeSerializable(
                getWrapper().getServerPropertyInfo(
                    locale,
                    wrapperProps,
                    serverProps));
    }

    public FarragoRJDriverPropertyInfo [] getColumnSetPropertyInfo(
        Locale locale,
        Properties wrapperProps,
        Properties serverProps,
        Properties tableProps)
        throws RemoteException
    {
        return
            makeSerializable(
                getWrapper().getColumnSetPropertyInfo(
                    locale,
                    wrapperProps,
                    serverProps,
                    tableProps));
    }

    public FarragoRJDriverPropertyInfo [] getColumnPropertyInfo(
        Locale locale,
        Properties wrapperProps,
        Properties serverProps,
        Properties tableProps,
        Properties columnProps)
        throws RemoteException
    {
        return
            makeSerializable(
                getWrapper().getColumnPropertyInfo(
                    locale,
                    wrapperProps,
                    serverProps,
                    tableProps,
                    columnProps));
    }

    public boolean isForeign()
        throws RemoteException
    {
        return getWrapper().isForeign();
    }

    /**
     * Gets wrapper information from the server.
     *
     * <p>This {@link FarragoMedDataWrapperInfo} is leak-proof -- unlike a
     * {@link FarragoMedDataWrapper}, we don't have to worry about freeing it.
     */
    private FarragoMedDataWrapperInfo getWrapper()
        throws RemoteException
    {
        try {
            return farragoConnection.getWrapper(mofId, libraryName, options);
        } catch (SQLException e) {
            throw new RemoteException("", e);
        }
    }

    private FarragoRJDriverPropertyInfo [] makeSerializable(
        DriverPropertyInfo [] infos)
    {
        FarragoRJDriverPropertyInfo [] dpis =
            new FarragoRJDriverPropertyInfo[infos.length];

        for (int i = 0; i < infos.length; i++) {
            dpis[i] = new FarragoRJDriverPropertyInfo(infos[i]);
        }
        return dpis;
    }
}

// End FarragoRJMedDataWrapperServer.java

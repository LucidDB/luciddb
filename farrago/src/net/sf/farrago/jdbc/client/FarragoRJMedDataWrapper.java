/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 SQLstream, Inc.
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2003-2007 John V. Sichi
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
package net.sf.farrago.jdbc.client;

import java.rmi.*;

import java.sql.*;

import java.util.*;

import net.sf.farrago.jdbc.*;
import net.sf.farrago.jdbc.rmi.*;


/**
 * Client-side JDBC implementation of {@link
 * net.sf.farrago.namespace.FarragoMedDataWrapper}.
 *
 * <p>It is paired with a <code>FarragoRJMedDataWrapperServer</code> via RMI.
 *
 * @author Tim Leung
 * @version $Id$
 */
class FarragoRJMedDataWrapper
    implements FarragoMedDataWrapperInfo,
        java.io.Serializable
{
    //~ Instance fields --------------------------------------------------------

    protected final FarragoRJMedDataWrapperInterface rmiDataWrapper_;

    //~ Constructors -----------------------------------------------------------

    public FarragoRJMedDataWrapper(FarragoRJMedDataWrapperInterface wrapper)
    {
        rmiDataWrapper_ = wrapper;
    }

    //~ Methods ----------------------------------------------------------------

    public DriverPropertyInfo [] getPluginPropertyInfo(
        Locale locale,
        Properties wrapperProps)
    {
        try {
            return getDriverPropertyInfo(
                rmiDataWrapper_.getPluginPropertyInfo(
                    locale,
                    wrapperProps));
        } catch (RemoteException e) {
            throw new RuntimeException(e.getMessage());
            // TODO: add 'throws SQLException' to interface, and throw new
            // SQLException(e.getMessage());
        }
    }

    public DriverPropertyInfo [] getServerPropertyInfo(
        Locale locale,
        Properties wrapperProps,
        Properties serverProps)
    {
        try {
            return getDriverPropertyInfo(
                rmiDataWrapper_.getServerPropertyInfo(
                    locale,
                    wrapperProps,
                    serverProps));
        } catch (RemoteException e) {
            throw new RuntimeException(e.getMessage());
            // TODO: add 'throws SQLException' to interface, and throw new
            // SQLException(e.getMessage());
        }
    }

    public DriverPropertyInfo [] getColumnSetPropertyInfo(
        Locale locale,
        Properties wrapperProps,
        Properties serverProps,
        Properties tableProps)
    {
        try {
            return getDriverPropertyInfo(
                rmiDataWrapper_.getColumnSetPropertyInfo(
                    locale,
                    wrapperProps,
                    serverProps,
                    tableProps));
        } catch (RemoteException e) {
            throw new RuntimeException(e.getMessage());
            // TODO: add 'throws SQLException' to interface, and throw new
            // SQLException(e.getMessage());
        }
    }

    public DriverPropertyInfo [] getColumnPropertyInfo(
        Locale locale,
        Properties wrapperProps,
        Properties serverProps,
        Properties tableProps,
        Properties columnProps)
    {
        try {
            return getDriverPropertyInfo(
                rmiDataWrapper_.getColumnPropertyInfo(
                    locale,
                    wrapperProps,
                    serverProps,
                    tableProps,
                    columnProps));
        } catch (RemoteException e) {
            throw new RuntimeException(e.getMessage());
            // TODO: add 'throws SQLException' to interface, and throw new
            // SQLException(e.getMessage());
        }
    }

    public boolean isForeign()
    {
        try {
            return rmiDataWrapper_.isForeign();
        } catch (RemoteException e) {
            throw new RuntimeException(e.getMessage());
            // TODO: add 'throws SQLException' to interface, and throw new
            // SQLException(e.getMessage());
        }
    }

    private DriverPropertyInfo [] getDriverPropertyInfo(
        FarragoRJDriverPropertyInfo [] infos)
    {
        DriverPropertyInfo [] dpis = new DriverPropertyInfo[infos.length];
        for (int i = 0; i < infos.length; i++) {
            dpis[i] = infos[i].getPropertyInfo();
        }
        return dpis;
    }
}

// End FarragoRJMedDataWrapper.java

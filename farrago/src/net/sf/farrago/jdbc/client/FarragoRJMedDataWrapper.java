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
package net.sf.farrago.jdbc.client;

import java.util.Locale;
import java.util.Properties;
import java.rmi.RemoteException;
import java.sql.SQLException;
import java.sql.DriverPropertyInfo;
import net.sf.farrago.namespace.FarragoMedDataWrapper;
import net.sf.farrago.namespace.FarragoMedDataServer;
import net.sf.farrago.catalog.FarragoRepos;
import net.sf.farrago.jdbc.rmi.FarragoRJMedDataWrapperInterface;

/**
 * Client-side JDBC implementation of {@link FarragoMedDataWrapper}.
 *
 * <p>It is paired with a
 * {@link net.sf.farrago.server.FarragoRJMedDataWrapperServer} via RMI.
 *
 * @author Tim Leung
 * @version $Id$
 */ 
class FarragoRJMedDataWrapper
    implements FarragoMedDataWrapper, java.io.Serializable
{

    net.sf.farrago.jdbc.rmi.FarragoRJMedDataWrapperInterface rmiDataWrapper_;

    public FarragoRJMedDataWrapper(net.sf.farrago.jdbc.rmi.FarragoRJMedDataWrapperInterface wrapper) {
        rmiDataWrapper_ = wrapper;
    }

    public DriverPropertyInfo [] getServerPropertyInfo(
        Locale locale,
        Properties wrapperProps,
        Properties serverProps)
    {
        try {
            return rmiDataWrapper_.getServerPropertyInfo(
                locale, wrapperProps, serverProps);
        } catch (RemoteException e) {
            throw new RuntimeException(e.getMessage());
            // TODO: add 'throws SQLException' to interface, and throw new SQLException(e.getMessage());
        }
    }

    public DriverPropertyInfo [] getColumnSetPropertyInfo(
        Locale locale,
        Properties wrapperProps,
        Properties serverProps,
        Properties tableProps)
    {
        try {
            return rmiDataWrapper_.getColumnSetPropertyInfo(
                locale, wrapperProps, serverProps, tableProps);
        } catch (RemoteException e) {
            throw new RuntimeException(e.getMessage());
            // TODO: add 'throws SQLException' to interface, and throw new SQLException(e.getMessage());
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
            return rmiDataWrapper_.getColumnPropertyInfo(
                locale, wrapperProps, serverProps, tableProps, columnProps);
        } catch (RemoteException e) {
            throw new RuntimeException(e.getMessage());
            // TODO: add 'throws SQLException' to interface, and throw new SQLException(e.getMessage());
        }
    }

    public FarragoMedDataServer newServer(
        String serverMofId,
        Properties props) throws SQLException
    {
        // TODO: Remove this method from the interface.
        throw new UnsupportedOperationException();
    }

    public boolean isForeign() {
        try {
            return rmiDataWrapper_.isForeign();
        } catch (RemoteException e) {
            throw new RuntimeException(e.getMessage());
            // TODO: add 'throws SQLException' to interface, and throw new SQLException(e.getMessage());
        }
    }

    public void initialize(FarragoRepos repos, Properties props)
        throws SQLException
    {
        // TODO: Remove this method from the interface.
        throw new UnsupportedOperationException();
    }

    public String getDescription(Locale locale) {
        try {
            return rmiDataWrapper_.getDescription(locale);
        } catch (RemoteException e) {
            throw new RuntimeException(e.getMessage());
            // TODO: add 'throws SQLException' to interface, and throw new SQLException(e.getMessage());
        }
    }

    public String getSuggestedName() {
        try {
            return rmiDataWrapper_.getSuggestedName();
        } catch (RemoteException e) {
            throw new RuntimeException(e.getMessage());
            // TODO: add 'throws SQLException' to interface, and throw new SQLException(e.getMessage());
        }
    }

    public DriverPropertyInfo[] getPluginPropertyInfo(
        Locale locale,
        Properties props)
    {
        try {
            return rmiDataWrapper_.getPluginPropertyInfo(locale, props);
        } catch (RemoteException e) {
            throw new RuntimeException(e.getMessage());
            // TODO: add 'throws SQLException' to interface, and throw new SQLException(e.getMessage());
        }
    }

    public void closeAllocation() {
        // REVIEW: Remove this method from the interface.
        try {
            rmiDataWrapper_.closeAllocation();
        } catch (RemoteException e) {
            System.out.println(e.getMessage());
        }
    }
}

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

import java.rmi.RemoteException;
import java.rmi.Remote;
import java.sql.DriverPropertyInfo;
import java.util.Locale;
import java.util.Properties;

/**
 * RMI server interface corresponding to
 * {@link net.sf.farrago.jdbc.FarragoMedDataWrapperInfo}.
 *
 * @author Tim Leung
 * @version $Id$
 */
public interface FarragoRJMedDataWrapperInterface extends Remote {
    /**
     * @see net.sf.farrago.jdbc.FarragoMedDataWrapperInfo#getServerPropertyInfo
     */
    DriverPropertyInfo [] getServerPropertyInfo(
        Locale locale,
        Properties wrapperProps,
        Properties serverProps)
        throws RemoteException;

    /**
     * @see net.sf.farrago.jdbc.FarragoMedDataWrapperInfo#getColumnSetPropertyInfo
     */
    DriverPropertyInfo [] getColumnSetPropertyInfo(
        Locale locale,
        Properties wrapperProps,
        Properties serverProps,
        Properties tableProps)
        throws RemoteException;

    /**
     * @see net.sf.farrago.jdbc.FarragoMedDataWrapperInfo#getColumnPropertyInfo
     */
    DriverPropertyInfo [] getColumnPropertyInfo(
        Locale locale,
        Properties wrapperProps,
        Properties serverProps,
        Properties tableProps,
        Properties columnProps)
        throws RemoteException;

    /**
     * @see net.sf.farrago.jdbc.FarragoMedDataWrapperInfo#isForeign
     */
    boolean isForeign() throws RemoteException;
}

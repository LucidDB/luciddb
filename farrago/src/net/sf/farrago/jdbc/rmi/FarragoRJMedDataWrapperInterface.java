/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 2004-2005 John V. Sichi
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later Eigenbase-approved version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307  USA
*/
package net.sf.farrago.jdbc.rmi;

import net.sf.farrago.jdbc.FarragoRJDriverPropertyInfo;
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
    FarragoRJDriverPropertyInfo [] getServerPropertyInfo(
        Locale locale,
        Properties wrapperProps,
        Properties serverProps)
        throws RemoteException;

    /**
     * @see net.sf.farrago.jdbc.FarragoMedDataWrapperInfo#getColumnSetPropertyInfo
     */
    FarragoRJDriverPropertyInfo [] getColumnSetPropertyInfo(
        Locale locale,
        Properties wrapperProps,
        Properties serverProps,
        Properties tableProps)
        throws RemoteException;

    /**
     * @see net.sf.farrago.jdbc.FarragoMedDataWrapperInfo#getColumnPropertyInfo
     */
    FarragoRJDriverPropertyInfo [] getColumnPropertyInfo(
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

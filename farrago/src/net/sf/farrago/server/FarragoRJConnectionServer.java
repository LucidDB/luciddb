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
package net.sf.farrago.server;

import org.objectweb.rmijdbc.RJConnectionServer;
import net.sf.farrago.jdbc.rmi.FarragoRJConnectionInterface;
import net.sf.farrago.jdbc.rmi.FarragoRJMedDataWrapperInterface;
import net.sf.farrago.jdbc.FarragoConnection;
import net.sf.farrago.namespace.FarragoMedDataWrapper;

import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.Properties;

/**
 * RMI server-side implementation of {@link java.sql.Connection},
 * also contains server-side implementations of the the extended methods of
 * a Farrago JDBC connection defined by the interface
 * {@link net.sf.farrago.jdbc.FarragoConnection}.
 *
 * @author Tim Leung
 * @version $Id$
 */
class FarragoRJConnectionServer extends RJConnectionServer
    implements FarragoRJConnectionInterface {
    /**
     * Holds the underlying connection. The underlying connection is also held
     * in the base class, but that member is private and is of the wrong type.
     */
    private final FarragoConnection farragoConnection;

    public FarragoRJConnectionServer(FarragoConnection c)
        throws RemoteException {
        super(c);
        this.farragoConnection = c;
    }

    public String findMofId(String wrapperName)
        throws RemoteException, SQLException {
        return farragoConnection.findMofId(wrapperName);
    }

    public FarragoRJMedDataWrapperInterface getWrapper(
        final String mofId,
        final String libraryName,
        Properties options)
        throws RemoteException, SQLException
    {
        return new FarragoRJMedDataWrapperServer(farragoConnection, mofId, 
            libraryName, options);
    }

}

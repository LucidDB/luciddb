/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 2004 John V. Sichi
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

import java.sql.*;

import java.util.*;

import net.sf.farrago.jdbc.*;
import net.sf.farrago.jdbc.rmi.*;

import org.objectweb.rmijdbc.*;


/**
 * RMI server-side implementation of {@link java.sql.Connection}, also contains
 * server-side implementations of the the extended methods of a Farrago JDBC
 * connection defined by the interface {@link
 * net.sf.farrago.jdbc.FarragoConnection}.
 *
 * @author Tim Leung
 * @version $Id$
 */
public class FarragoRJConnectionServer
    extends RJConnectionServer
    implements FarragoRJConnectionInterface
{
    //~ Instance fields --------------------------------------------------------

    /**
     * Holds the underlying connection. The underlying connection is also held
     * in the base class, but that member is private and is of the wrong type.
     */
    private final FarragoConnection farragoConnection;

    //~ Constructors -----------------------------------------------------------

    public FarragoRJConnectionServer(FarragoConnection c)
        throws RemoteException
    {
        super(c);
        this.farragoConnection = c;
    }

    //~ Methods ----------------------------------------------------------------

    public String findMofId(String wrapperName)
        throws RemoteException, SQLException
    {
        return farragoConnection.findMofId(wrapperName);
    }

    public long getFarragoSessionId()
        throws RemoteException, SQLException
    {
        return farragoConnection.getFarragoSessionId();
    }

    public FarragoRJMedDataWrapperInterface getWrapper(
        final String mofId,
        final String libraryName,
        Properties options)
        throws RemoteException, SQLException
    {
        return new FarragoRJMedDataWrapperServer(
            farragoConnection,
            mofId,
            libraryName,
            options);
    }
}

// End FarragoRJConnectionServer.java

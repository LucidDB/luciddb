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

import net.sf.farrago.jdbc.FarragoConnection;
import org.objectweb.rmijdbc.RJConnectionInterface;
import org.objectweb.rmijdbc.RJDriverServer;
import org.objectweb.rmijdbc.RJDriverInterface;

import java.rmi.RemoteException;
import java.sql.Connection;

/**
 * RMI server-side implementation of {@link java.sql.Driver}.
 *
 * @author Tim Leung
 * @version $Id$
 */
class FarragoRJDriverServer extends RJDriverServer
    implements RJDriverInterface
{
    public FarragoRJDriverServer(String admpasswd) throws RemoteException
    {
        super(admpasswd);
    }

    protected RJConnectionInterface buildConnectionServer(Connection c)
        throws RemoteException
    {
        if (c instanceof FarragoConnection) {
            return new FarragoRJConnectionServer((FarragoConnection) c);
        }
        return super.buildConnectionServer(c);
    }
}

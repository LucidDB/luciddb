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

import net.sf.farrago.jdbc.FarragoConnection;
import net.sf.farrago.jdbc.FarragoMedDataWrapperInfo;
import net.sf.farrago.jdbc.rmi.FarragoRJConnectionInterface;
import net.sf.farrago.jdbc.rmi.FarragoRJMedDataWrapperInterface;
import org.objectweb.rmijdbc.RJConnection;
import org.objectweb.rmijdbc.RJConnectionInterface;
import org.objectweb.rmijdbc.RJDriverInterface;

import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.Properties;

/**
 * JDBC connection to Farrago across an RMI transport.
 *
 * <p>It is paired with an <code>FarragoRJConnectionServer</code> via RMI.
 *
 * @author Tim Leung
 * @version $Id$
 */
class FarragoRJConnection extends RJConnection
    implements java.io.Serializable, FarragoConnection
{
    FarragoRJConnection(RJConnectionInterface rmiconn)
    {
        super(rmiconn);
    }

    public FarragoRJConnection(
        RJDriverInterface drv,
        String url,
        Properties info) throws Exception
    {
        super(drv,url,info);
    }

    private FarragoRJConnectionInterface getFarragoRmiCon() {
        return (FarragoRJConnectionInterface) rmiConnection_;
    }

    public String findMofId(String wrapperName)
        throws SQLException
    {
        try {
            return getFarragoRmiCon().findMofId(wrapperName);
        } catch (RemoteException e) {
            throw new SQLException(e.getMessage());
        }
    }

    public FarragoMedDataWrapperInfo getWrapper(
        String mofId,
        String libraryName,
        Properties options)
        throws SQLException
    {
        try {
            final FarragoRJMedDataWrapperInterface wrapper =
                getFarragoRmiCon().getWrapper(mofId, libraryName, options);
            return new FarragoRJMedDataWrapper(wrapper);
        } catch (RemoteException e) {
            throw new SQLException(e.getMessage());
        }
    }
}

/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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
public class FarragoRJConnection extends RJConnection
    implements java.io.Serializable, FarragoConnection
{
    /** SerialVersionUID created with JDK 1.5 serialver tool. */
    private static final long serialVersionUID = -3256212096290593733L;

    protected FarragoRJConnection(RJConnectionInterface rmiconn)
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

    public long getFarragoSessionId() throws SQLException
    {
        try {
            return getFarragoRmiCon().getFarragoSessionId();
        } catch (RemoteException e) {
            throw new SQLException(e.getMessage());
        }
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

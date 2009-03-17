/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
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

import org.eigenbase.jdbc4.*;

import org.objectweb.rmijdbc.*;


/**
 * JDBC connection to Farrago across an RMI transport.
 *
 * <p>It is paired with an <code>FarragoRJConnectionServer</code> via RMI.
 *
 * @author Tim Leung
 * @version $Id$
 */
public class FarragoRJConnection
    extends UnwrappableRJConnection
    implements java.io.Serializable,
        FarragoConnection
{
    //~ Static fields/initializers ---------------------------------------------

    /**
     * SerialVersionUID created with JDK 1.5 serialver tool.
     */
    private static final long serialVersionUID = -3256212096290593733L;

    //~ Constructors -----------------------------------------------------------

    protected FarragoRJConnection(RJConnectionInterface rmiconn)
    {
        super(rmiconn);
    }

    public FarragoRJConnection(
        RJDriverInterface drv,
        String url,
        Properties info)
        throws Exception
    {
        super(drv, url, info);
    }

    //~ Methods ----------------------------------------------------------------

    private FarragoRJConnectionInterface getFarragoRmiCon()
    {
        return (FarragoRJConnectionInterface) rmiConnection_;
    }

    public long getFarragoSessionId()
        throws SQLException
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

    //
    // begin JDBC 4 methods
    //

    // implement Connection
    public Struct createStruct(String typeName, Object [] attributes)
        throws SQLException
    {
        throw new UnsupportedOperationException("createStruct");
    }

    // implement Connection
    public Array createArrayOf(String typeName, Object [] elements)
        throws SQLException
    {
        throw new UnsupportedOperationException("createArrayOf");
    }

    // implement Connection
    public Properties getClientInfo()
        throws SQLException
    {
        throw new UnsupportedOperationException("getClientInfo");
    }

    // implement Connection
    public String getClientInfo(String name)
        throws SQLException
    {
        throw new UnsupportedOperationException("getClientInfo");
    }

    // implement Connection
    public void setClientInfo(String name, String value)
    {
        throw new UnsupportedOperationException("setClientInfo");
    }

    // implement Connection
    public void setClientInfo(Properties props)
    {
        throw new UnsupportedOperationException("setClientInfo");
    }

    // implement Connection
    public boolean isValid(int timeout)
    {
        throw new UnsupportedOperationException("isValid");
    }

    // implement Connection
    public SQLXML createSQLXML()
        throws SQLException
    {
        throw new UnsupportedOperationException("createSQLXML");
    }

    // implement Connection
    public NClob createNClob()
        throws SQLException
    {
        throw new UnsupportedOperationException("createNClob");
    }

    // implement Connection
    public Clob createClob()
        throws SQLException
    {
        throw new UnsupportedOperationException("createClob");
    }

    // implement Connection
    public Blob createBlob()
        throws SQLException
    {
        throw new UnsupportedOperationException("createBlob");
    }

    //
    // end JDBC 4 methods
    //
}

// End FarragoRJConnection.java

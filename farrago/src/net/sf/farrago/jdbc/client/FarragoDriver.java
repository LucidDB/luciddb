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

import org.objectweb.rmijdbc.Driver;
import org.objectweb.rmijdbc.RJConnectionInterface;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * FarragoDriver extends the Driver implementation in RmiJdbc package
 *
 * @author Tim Leung
 * @version $Id$
 */
public class FarragoDriver extends org.objectweb.rmijdbc.Driver 
    implements java.sql.Driver, java.io.Serializable 
{
    public FarragoDriver() throws Exception { super(); }

    public java.sql.Connection buildConnection(RJConnectionInterface c)
    throws SQLException {
        return new FarragoRJConnection(c);
    }
};

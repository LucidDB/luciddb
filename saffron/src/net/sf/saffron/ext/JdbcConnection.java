/*
// Saffron preprocessor and data engine.
// Copyright (C) 2002-2004 Disruptive Tech
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
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

package net.sf.saffron.ext;

import org.eigenbase.relopt.RelOptConnection;
import org.eigenbase.relopt.RelOptSchema;


/**
 * A <code>JdbcConnection</code> is an implementation of {@link
 * RelOptConnection} which gets its data from a JDBC database.
 *
 * <p>
 * Derived classes must implement {@link #getRelOptSchema} and {@link
 * #getRelOptSchemaStatic}.
 * </p>
 *
 * @author jhyde
 * @version $Id$
 *
 * @since 10 November, 2001
 */
public abstract class JdbcConnection implements RelOptConnection
{
    java.sql.Connection sqlConnection;

    public JdbcConnection(java.sql.Connection sqlConnection)
    {
        this.sqlConnection = sqlConnection;
    }

    public void setConnection(java.sql.Connection sqlConnection)
    {
        this.sqlConnection = sqlConnection;
    }

    public java.sql.Connection getConnection()
    {
        return sqlConnection;
    }

    // for Connection
    public static RelOptSchema getRelOptSchemaStatic()
    {
        throw new UnsupportedOperationException(
            "Derived class must implement "
            + "public static Schema getRelOptSchemaStatic()");
    }

    // implement Connection
    public Object contentsAsArray(
        String qualifier,
        String tableName)
    {
        throw new UnsupportedOperationException(
            "JdbcConnection.contentsAsArray() should have been replaced");
    }
}


// End JdbcConnection.java

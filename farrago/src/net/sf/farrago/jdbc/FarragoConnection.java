/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2004-2007 John V. Sichi
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
package net.sf.farrago.jdbc;

import java.sql.*;

import java.util.*;


/**
 * JDBC connection to Farrago.
 *
 * <p>This interface extends the usual {@link java.sql.Connection} interface to
 * include methods for interrogating SQL/MED wrappers and data servers, and to
 * expose the farrago session identifier.
 *
 * @author jhyde
 * @version $Id$
 */
public interface FarragoConnection
    extends java.sql.Connection
{
    //~ Methods ----------------------------------------------------------------

    long getFarragoSessionId()
        throws SQLException;

    String findMofId(String wrapperName)
        throws SQLException;

    FarragoMedDataWrapperInfo getWrapper(
        String mofId,
        String libraryName,
        Properties options)
        throws SQLException;
}

// End FarragoConnection.java

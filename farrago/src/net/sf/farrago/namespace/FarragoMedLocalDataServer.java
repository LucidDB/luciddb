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
package net.sf.farrago.namespace;

import java.sql.*;

import net.sf.farrago.fem.med.*;
import net.sf.farrago.fennel.*;


/**
 * FarragoMedLocalDataServer represents a {@link FarragoMedDataServer}
 * instance originating from a {@link FarragoMedDataWrapper} managing
 * local data.  It defines extra methods not relevant in the context
 * of foreign data.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface FarragoMedLocalDataServer extends FarragoMedDataServer
{
    //~ Methods ---------------------------------------------------------------

    /**
     * Sets the Fennel database handle to use for accessing local storage.
     *
     * @param fennelDbHandle the handle to use
     */
    public void setFennelDbHandle(FennelDbHandle fennelDbHandle);

    /**
     * Creates an index.
     *
     * @param index definition of the index to create
     *
     * @return root PageId of index
     */
    public long createIndex(FemLocalIndex index)
        throws SQLException;

    /**
     * Drops or truncates an index.
     *
     * @param index definition of the index to drop
     *
     * @param rootPageid root PageId of index
     *
     * @param truncate if true, only truncate storage; if false, drop storage
     * entirely
     */
    public void dropIndex(
        FemLocalIndex index,
        long rootPageId,
        boolean truncate)
        throws SQLException;
}


// End FarragoMedLocalDataServer.java

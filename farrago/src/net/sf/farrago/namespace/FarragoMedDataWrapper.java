/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 2003 John V. Sichi
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
package net.sf.farrago.namespace;

import java.sql.*;

import java.util.*;

import net.sf.farrago.jdbc.*;
import net.sf.farrago.plugin.*;


/**
 * FarragoMedDataWrapper defines an interface for accessing foreign or local
 * data. It is a non-standard replacement for the standard SQL/MED internal
 * interface.
 *
 * <p>Implementations of FarragoMedDataWrapper must provide a public default
 * constructor in order to be loaded via the CREATE {FOREIGN|LOCAL} DATA WRAPPER
 * statement. FarragoMedDataWrapper extends FarragoAllocation; when
 * closeAllocation is called, all resources (such as connections) used to access
 * the data should be released.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface FarragoMedDataWrapper
    extends FarragoPlugin,
        FarragoMedDataWrapperInfo
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Creates an instance of this wrapper for a particular server. This
     * supports the SQL/MED CREATE SERVER statement. The TYPE and VERSION
     * attributes are rolled in with the other properties. As much validation as
     * possible should be performed, including establishing connections if
     * appropriate.
     *
     * <p>If this wrapper returns false from the isForeign method, then returned
     * server instances must implement the FarragoMedLocalDataServer interface.
     *
     * @param serverMofId MOFID of server definition in repository; this can be
     * used for accessing the server definition from generated code
     * @param props server properties
     *
     * @return new server instance
     *
     * @exception SQLException if server connection is unsuccessful
     */
    public FarragoMedDataServer newServer(
        String serverMofId,
        Properties props)
        throws SQLException;

    /**
     * Returns whether server supports sharing by multiple threads. Used by
     * {@link net.sf.farrago.namespace.util.FarragoDataWrapperCache#loadServer}
     * to determine if the entry should be exclusive (not shared).
     *
     * @return true only if server sharing is supported
     */
    public boolean supportsServerSharing();
}

// End FarragoMedDataWrapper.java

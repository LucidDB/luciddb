/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
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

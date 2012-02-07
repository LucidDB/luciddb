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
package net.sf.farrago.namespace.mql;

import java.sql.*;

import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.impl.*;
import net.sf.farrago.resource.*;


/**
 * MedMqlForeignDataWrapper provides an implementation of the {@link
 * FarragoMedDataWrapper} interface for MQL.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class MedMqlForeignDataWrapper
    extends MedAbstractDataWrapper
{
    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new data wrapper instance.
     */
    public MedMqlForeignDataWrapper()
    {
    }

    //~ Methods ----------------------------------------------------------------

    // implement FarragoMedDataWrapper
    public String getSuggestedName()
    {
        return "MQL_FOREIGN_DATA_WRAPPER";
    }

    // implement FarragoMedDataWrapper
    public String getDescription(Locale locale)
    {
        // TODO jvs 6-Jan-2008:  i18n
        return "Foreign data wrapper for MQL";
    }

    // TODO jvs 6-Jan-2008:  implement getServerPropertyInfo

    // implement FarragoMedDataWrapper
    public FarragoMedDataServer newServer(
        String serverMofId,
        Properties props)
        throws SQLException
    {
        MedMqlDataServer server =
            new MedMqlDataServer(
                this,
                serverMofId,
                props);
        boolean success = false;
        try {
            server.initialize();
            success = true;
            return server;
        } finally {
            if (!success) {
                server.closeAllocation();
            }
        }
    }
}

// End MedMqlForeignDataWrapper.java

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
package org.eigenbase.applib.analysis;

import java.sql.*;

import org.eigenbase.applib.resource.*;
import org.eigenbase.applib.util.*;


/**
 * ComputeStatisticsForSchema UDP calls 'analyze table ... ' with 'compute
 * statistics for all columns' for every table in a schema.
 *
 * @author Oscar Gothberg
 * @version $Id$
 * @see EstimateStatisticsForSchemaUdp
 */

public abstract class ComputeStatisticsForSchemaUdp
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Compute statistics for every table in the given schema.
     *
     * @param schemaName name of schema to compute statistics for
     */
    public static void execute(String schemaName)
        throws SQLException
    {
        // build statement, forward it to DoForEntireSchemaUdp
        DoForEntireSchemaUdp.execute(
            "analyze table %TABLE_NAME% compute statistics for all columns",
            schemaName,
            "TABLES");
    }
}

// End ComputeStatisticsForSchemaUdp.java

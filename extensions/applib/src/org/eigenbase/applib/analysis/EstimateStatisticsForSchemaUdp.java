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

import java.math.*;

import java.sql.*;

import org.eigenbase.applib.resource.*;
import org.eigenbase.applib.util.*;


/**
 * EstimateStatisticsForSchema UDP calls 'analyze table ... ' with 'estimate
 * statistics for all columns' for every table in the schema. A fixed sampling
 * rate, used for all tables, may also be specified.
 *
 * @author Stephan Zuercher
 * @version $Id$
 * @see ComputeStatisticsForSchemaUdp
 */
public abstract class EstimateStatisticsForSchemaUdp
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Estimates statistics for all tables in the given schema with the default
     * sampling rates.
     *
     * @param schemaName name of schema to estimate statistics for
     */
    public static void execute(String schemaName)
        throws SQLException
    {
        analyze(schemaName, null);
    }

    /**
     * Estimates statistics for all tables in the given schema with the given
     * sampling rate. If sampling rate is null, uses the default rates as in
     * {@link #execute(String)}.
     *
     * @param schemaName name of schema to estimate statistics for
     * @param samplingRate sampling rate to use for statistics estimation
     */
    public static void execute(String schemaName, Double samplingRate)
        throws SQLException
    {
        analyze(schemaName, samplingRate);
    }

    private static void analyze(String schemaName, Double samplingRate)
        throws SQLException
    {
        // build statement, forward it to DoForEntireSchemaUdp
        String sql =
            "analyze table %TABLE_NAME% estimate statistics for all columns";
        if (samplingRate != null) {
            BigDecimal dec = new BigDecimal(samplingRate);
            sql += " sample " + dec.toPlainString() + " percent";
        }

        DoForEntireSchemaUdp.execute(sql, schemaName, "TABLES");
    }
}

// End EstimateStatisticsForSchemaUdp.java

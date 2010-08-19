/*
// $Id$
// Applib is a library of SQL-invocable routines for Eigenbase applications.
// Copyright (C) 2008 The Eigenbase Project
// Copyright (C) 2008 SQLstream, Inc.
// Copyright (C) 2008 DynamoBI Corporation
//
// This library is free software; you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation; either version 2.1 of the License, or (at
// your option) any later version.
//
// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public
// License along with this library; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
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

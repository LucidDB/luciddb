/*
// $Id$
// LucidDB is a DBMS optimized for business intelligence.
// Copyright (C) 2008-2008 LucidEra, Inc.
// Copyright (C) 2008-2008 The Eigenbase Project
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

package com.lucidera.luciddb.applib.analysis;

import com.lucidera.luciddb.applib.resource.*;
import com.lucidera.luciddb.applib.util.*;
import java.math.*;
import java.sql.*;

/**
 * EstimateStatisticsForSchema UDP calls 'analyze table ... ' with 'estimate
 * statistics for all columns' for every table in the schema.  A fixed
 * sampling rate, used for all tables, may also be specified.
 *
 * @see ComputeStatisticsForSchemaUdp
 * @author Stephan Zuercher
 * @version $Id$
 */
public abstract class EstimateStatisticsForSchemaUdp
{
    /**
     * Estimates statistics for all tables in the given schema with the default
     * sampling rates.
     *
     * @param schemaName name of schema to estimate statistics for
     */
    public static void execute(String schemaName) throws SQLException
    {
        analyze(schemaName, null);
    }

    /**
     * Estimates statistics for all tables in the given schema with the given
     * sampling rate.  If sampling rate is null, uses the default rates
     * as in {@link #execute(String)}.
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

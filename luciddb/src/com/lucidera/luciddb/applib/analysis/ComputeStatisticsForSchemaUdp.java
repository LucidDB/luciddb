/*
// $Id$
// LucidDB is a DBMS optimized for business intelligence.
// Copyright (C) 2006-2007 LucidEra, Inc.
// Copyright (C) 2006-2007 The Eigenbase Project
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
import com.lucidera.luciddb.applib.util.DoForEntireSchemaUdp;
import java.sql.*;

/**
 * ComputeStatisticsForSchema UDP calls 'analyze table ... ' with 'compute
 * statistics for all columns' for every table in a schema.
 *
 * @see EstimateStatisticsForSchemaUdp
 * @author Oscar Gothberg
 * @version $Id$
 */

public abstract class ComputeStatisticsForSchemaUdp {

    /**
     * Compute statistics for every table in the given schema.
     *
     * @param schemaName name of schema to compute statistics for
     */
    public static void execute(String schemaName) throws SQLException
    {
        // build statement, forward it to DoForEntireSchemaUdp
        DoForEntireSchemaUdp.execute(
            "analyze table %TABLE_NAME% compute statistics for all columns", 
            schemaName,
            "TABLES");
    }
}

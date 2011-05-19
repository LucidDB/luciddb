/*
// $Id$
// Applib is a library of SQL-invocable routines for Eigenbase applications.
// Copyright (C) 2006 The Eigenbase Project
// Copyright (C) 2006 SQLstream, Inc.
// Copyright (C) 2006 DynamoBI Corporation
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

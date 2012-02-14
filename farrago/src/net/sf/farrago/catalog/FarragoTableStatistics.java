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
package net.sf.farrago.catalog;

import java.sql.*;

import java.util.*;

import net.sf.farrago.cwm.core.*;
import net.sf.farrago.fem.sql2003.*;

import org.eigenbase.sarg.*;
import org.eigenbase.stat.*;


/**
 * This class reads statistics for a Farrago table from data stored in the
 * catalog.
 *
 * @author John Pham
 * @version $Id$
 */
public class FarragoTableStatistics
    implements RelStatSource
{
    //~ Instance fields --------------------------------------------------------

    private FarragoRepos repos;
    private FemAbstractColumnSet table;
    private Timestamp labelTimestamp;

    //~ Constructors -----------------------------------------------------------

    /**
     * Initialize an object for retrieving table statistics
     *
     * @param repos the repository containing stats
     * @param table the table for which to retrieve stats
     *
     * @deprecated
     */
    public FarragoTableStatistics(
        FarragoRepos repos,
        FemAbstractColumnSet table)
    {
        this(repos, table, null);
    }

    /**
     * Initialize an object for retrieving table statistics, optionally based on
     * a label setting.
     *
     * @param repos the repository containing stats
     * @param table the table for which to retrieve stats
     * @param labelTimestamp the creation timestamp of the label that determines
     * which stats to retrieve; null if there is no label setting
     */
    public FarragoTableStatistics(
        FarragoRepos repos,
        FemAbstractColumnSet table,
        Timestamp labelTimestamp)
    {
        this.repos = repos;
        this.table = table;
        this.labelTimestamp = labelTimestamp;
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelStatSource
    public Double getRowCount()
    {
        Long [] rowCounts = new Long[2];
        FarragoCatalogUtil.getRowCounts(
            table,
            labelTimestamp,
            rowCounts);
        return (rowCounts[0] == null) ? null : Double.valueOf(rowCounts[0]);
    }

    // implement RelStatSource
    public RelStatColumnStatistics getColumnStatistics(
        int ordinal,
        SargIntervalSequence predicate)
    {
        List<CwmFeature> features = table.getFeature();
        FemAbstractColumn column = (FemAbstractColumn) features.get(ordinal);

        FarragoColumnHistogram result =
            new FarragoColumnHistogram(column, predicate, labelTimestamp);
        result.evaluate();
        return result;
    }
}

// End FarragoTableStatistics.java

/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
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

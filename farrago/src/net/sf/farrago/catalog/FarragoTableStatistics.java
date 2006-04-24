/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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

import java.util.*;

import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.cwm.keysindexes.CwmIndexedFeature;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fem.sql2003.*;

import org.eigenbase.rel.*;
import org.eigenbase.sarg.*;
import org.eigenbase.stat.*;

/**
 * This class reads statistics for a Farrago table from data stored in 
 * the catalog.
 *
 * @author John Pham
 * @version $Id$
 */
public class FarragoTableStatistics implements RelStatSource
{
    private FarragoRepos repos;
    private FemAbstractColumnSet table;
    
    /**
     * Initialize an object for retrieving table statistics
     * 
     * @param repos the repository containing stats
     * @param table the table for which to retrieve stats
     */
    public FarragoTableStatistics(
        FarragoRepos repos, 
        FemAbstractColumnSet table) 
    {
        this.repos = repos;
        this.table = table;
    }

    // implement RelStatSource
    public Double getRowCount() {
        Long rowCount = table.getRowCount();
        return (rowCount == null) ? null : Double.valueOf(rowCount);
    }
    
    // implement RelStatSource
    public RelStatColumnStatistics getColumnStatistics(
        int ordinal,
        SargIntervalSequence predicate)
    {
        List features = table.getFeature();
        FemAbstractColumn column = 
            (FemAbstractColumn) features.get(ordinal);

        FarragoColumnHistogram result =
            new FarragoColumnHistogram(column, predicate);
        result.evaluate();
        return result;
    }
}

// End FarragoTableStatistics.java

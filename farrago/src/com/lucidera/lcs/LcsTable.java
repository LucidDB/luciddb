/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 LucidEra, Inc.
// Copyright (C) 2005-2005 The Eigenbase Project
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
package com.lucidera.lcs;

import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.namespace.impl.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;


/**
 * An implementation of RelOptTable for accessing data in a LucidDB
 * column-store.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class LcsTable
    extends MedAbstractColumnSet
{

    //~ Instance fields --------------------------------------------------------

    /**
     * Helper class to manipulate the cluster indexes.
     */
    private LcsIndexGuide indexGuide;

    private List<FemLocalIndex> clusteredIndexes;

    //~ Constructors -----------------------------------------------------------

    LcsTable(
        String [] localName,
        RelDataType rowType,
        Properties tableProps,
        Map<String, Properties> columnPropMap)
    {
        super(localName, null, rowType, tableProps, columnPropMap);
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptTable
    public RelNode toRel(
        RelOptCluster cluster,
        RelOptConnection connection)
    {
        RelNode [] emptyInput = new RelNode[0];

        clusteredIndexes =
            FarragoCatalogUtil.getClusteredIndexes(
                getPreparingStmt().getRepos(),
                getCwmColumnSet());
        return
            new LcsRowScanRel(
                cluster,
                emptyInput,
                this,
                clusteredIndexes,
                connection,
                null,
                true,
                false);
    }

    public LcsIndexGuide getIndexGuide()
    {
        if (indexGuide == null) {
            indexGuide =
                new LcsIndexGuide(
                    getPreparingStmt().getFarragoTypeFactory(),
                    getCwmColumnSet());
        }
        return indexGuide;
    }

    public List<FemLocalIndex> getClusteredIndexes()
    {
        if (clusteredIndexes == null) {
            clusteredIndexes =
                FarragoCatalogUtil.getClusteredIndexes(
                    getPreparingStmt().getRepos(),
                    getCwmColumnSet());
        }
        return clusteredIndexes;
    }
}

// End LcsTable.java

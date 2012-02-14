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
package org.luciddb.lcs;

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

        // Sort the clusters by cluster name
        TreeSet<FemLocalIndex> sortedClusters =
            new TreeSet<FemLocalIndex>(new ClusterComparator());
        sortedClusters.addAll(
            FarragoCatalogUtil.getClusteredIndexes(
                getPreparingStmt().getRepos(),
                getCwmColumnSet()));
        clusteredIndexes = new ArrayList<FemLocalIndex>(sortedClusters);

        double inputSelectivity = 1.0;

        return new LcsRowScanRel(
            cluster,
            emptyInput,
            this,
            clusteredIndexes,
            connection,
            null,
            true,
            new Integer[0],
            inputSelectivity);
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

    /**
     * Comparator that sorts clusters by cluster name.
     */
    private static class ClusterComparator
        implements Comparator<FemLocalIndex>
    {
        public int compare(FemLocalIndex i1, FemLocalIndex i2)
        {
            return i1.getName().compareTo(i2.getName());
        }
    }
}

// End LcsTable.java

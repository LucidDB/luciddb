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

import java.sql.*;
import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.cwm.keysindexes.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.fennel.tuple.*;
import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.impl.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.query.*;
import net.sf.farrago.type.*;
import net.sf.farrago.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.convert.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.util.*;


/**
 * LcsDataServer implements the {@link FarragoMedDataServer} interface
 * for LucidDB column-store data.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class LcsDataServer extends MedAbstractFennelDataServer
{
    //~ Constructors ----------------------------------------------------------

    LcsDataServer(
        String serverMofId,
        Properties props,
        FarragoRepos repos)
    {
        super(serverMofId, props, repos);
    }

    //~ Methods ---------------------------------------------------------------

    // implement FarragoMedDataServer
    public FarragoMedColumnSet newColumnSet(
        String [] localName,
        Properties tableProps,
        FarragoTypeFactory typeFactory,
        RelDataType rowType,
        Map columnPropMap)
        throws SQLException
    {
        return new LcsTable(localName, rowType, tableProps, columnPropMap);
    }

    // implement FarragoMedDataServer
    public void registerRules(RelOptPlanner planner)
    {
        super.registerRules(planner);

        // NOTE jvs 26-Oct-2005:  planner rules specific to
        // column-store go here
        planner.addRule(new LcsTableAppendRule());
        planner.addRule(new LcsTableProjectionRule());
        planner.addRule(new LcsIndexBuilderRule());
        planner.addRule(new LcsIndexAccessRule());
    }

    // implement FarragoMedLocalDataServer
    public void validateTableDefinition(
        FemLocalTable table,
        FemLocalIndex generatedPrimaryKeyIndex)
        throws SQLException
    {
        // Verify that no column has a collection for its type, because
        // we don't support those...yet.
        Set<CwmColumn> uncoveredColumns = new HashSet<CwmColumn>(
            table.getFeature());
        for (CwmColumn col : uncoveredColumns) {
            if (col.getType() instanceof FemSqlcollectionType) {
                throw Util.needToImplement(
                    "column-store for collection type");
            }
        }
        
        // Verify that clustered indexes do not overlap
        for (Object i : FarragoCatalogUtil.getTableIndexes(repos, table)) {
            FemLocalIndex index = (FemLocalIndex) i;
            if (!index.isClustered()) {
                continue;
            }
            // LCS clustered indexes are sorted on RID, not value
            index.setSorted(false);
            for (Object f : index.getIndexedFeature()) {
                CwmIndexedFeature indexedFeature = (CwmIndexedFeature) f;
                if (!uncoveredColumns.contains(indexedFeature.getFeature())) {
                    throw FarragoResource.instance().
                        ValidatorMultipleClusterForColumn.ex(
                            repos.getLocalizedObjectName(
                                indexedFeature.getFeature()));
                }
                uncoveredColumns.remove(indexedFeature.getFeature());
            }
        }

        // Create system-defined clustered indexes for any columns which aren't
        // covered by user-defined clustered indexes.
        for (CwmColumn col : uncoveredColumns) {
            FemLocalIndex index = repos.newFemLocalIndex();
            String name =
                "SYS$CLUSTERED_INDEX$" + table.getNamespace().getName()
                + "$" + table.getName() + "$" + col.getName();
            index.setName(
                FarragoCatalogUtil.uniquifyGeneratedName(repos, col, name));
            index.setSpannedClass(table);
            index.setClustered(true);
            index.setSorted(false);
            FemLocalIndexColumn indexColumn = repos.newFemLocalIndexColumn();
            indexColumn.setName(col.getName());
            indexColumn.setFeature(col);
            indexColumn.setIndex(index);
            indexColumn.setOrdinal(0);
        }
    }

    // implement FarragoMedLocalDataServer
    public RelNode constructIndexBuildPlan(
        RelOptTable table,
        FemLocalIndex index,
        RelOptCluster cluster)
    {
        if (index.isClustered()) {
            // TODO: is this supported?
            throw Util.needToImplement("new cluster on existing LCS rows");
        }
        
        LcsIndexGuide indexGuide = new LcsIndexGuide(
            (FarragoTypeFactoryImpl) cluster.getTypeFactory(),
            FarragoCatalogUtil.getIndexTable(index),
            index);

        //
        // Unclustered Lcs indexes are implemented as bitmap indexes.
        // To construct bitmap indexes, we pass initiation parameters
        // to the generators as a row of data.
        // 
        OneRowRel oneRowRel = new OneRowRel(cluster);
        RexNode[] inputValues = 
            indexGuide.getUnclusteredInputs(cluster.getRexBuilder());
        RelDataType rowType = indexGuide.getUnclusteredInputType();
        ProjectRel generatorInputs =
            new ProjectRel(
                cluster,
                oneRowRel,
                inputValues,
                rowType,
                ProjectRel.Flags.Boxed);

        return new FarragoIndexBuilderRel(
            cluster, table, generatorInputs, index);
    }
    
    protected void prepareIndexCmd(
        FemIndexCmd cmd,
        FemLocalIndex index)
    {
        // FIXME: should not create new type factory
        LcsIndexGuide indexGuide = new LcsIndexGuide(
            new FarragoTypeFactoryImpl(repos),
            FarragoCatalogUtil.getIndexTable(index));
        if (index.isClustered()) {
            prepareClusteredIndexCmd(
                indexGuide,
                cmd,
                index);
        } else {
            prepareUnclusteredIndexCmd(
                indexGuide,
                cmd,
                index);
        }
    }

    private void prepareClusteredIndexCmd(
        LcsIndexGuide indexGuide,
        FemIndexCmd cmd,
        FemLocalIndex index)
    {
        cmd.setTupleDesc(
            indexGuide.createClusteredBTreeTupleDesc());
        
        cmd.setKeyProj(
            indexGuide.createClusteredBTreeRidDesc());

        // Tell Fennel how to drop the cluster pages together with
        // the BTree.
        cmd.setLeafPageIdProj(
            indexGuide.createClusteredBTreePageIdDesc());
    }
    
    private void prepareUnclusteredIndexCmd(
        LcsIndexGuide indexGuide,
        FemIndexCmd cmd,
        FemLocalIndex index)
    {
        cmd.setTupleDesc(
            indexGuide.createUnclusteredBTreeTupleDesc(index));
        cmd.setKeyProj(
            indexGuide.createUnclusteredBTreeKeyProj(index));
    }
}


// End LcsDataServer.java

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
import net.sf.farrago.cwm.keysindexes.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.fennel.*;
import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.impl.*;
import net.sf.farrago.query.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.type.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.rules.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.util.*;


/**
 * LcsDataServer implements the {@link FarragoMedDataServer} interface for
 * LucidDB column-store data.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class LcsDataServer
    extends MedAbstractFennelDataServer
{
    //~ Constructors -----------------------------------------------------------

    LcsDataServer(
        String serverMofId,
        Properties props,
        FarragoRepos repos)
    {
        super(serverMofId, props, repos);
    }

    //~ Methods ----------------------------------------------------------------

    // implement FarragoMedDataServer
    public FarragoMedColumnSet newColumnSet(
        String [] localName,
        Properties tableProps,
        FarragoTypeFactory typeFactory,
        RelDataType rowType,
        Map<String, Properties> columnPropMap)
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
        planner.addRule(new LcsTableDeleteRule());
        planner.addRule(new LcsTableProjectionRule());
        planner.addRule(new LcsIndexBuilderRule());

        // multiple sub-rules need to be specified for this rule
        // because we need to distinguish the cases where there are
        // children below the rowscan; note that the rule is very
        // specific to speed up matching

        // IndexSemiJoin rules
        planner.addRule(
            new LcsIndexSemiJoinRule(
                new RelOptRuleOperand(
                    SemiJoinRel.class,
                    new RelOptRuleOperand[] {
                        new RelOptRuleOperand(LcsRowScanRel.class, null)
                    }),
                "without index child"));
        planner.addRule(
            new LcsIndexSemiJoinRule(
                new RelOptRuleOperand(
                    SemiJoinRel.class,
                    new RelOptRuleOperand[] {
                        new RelOptRuleOperand(
                            LcsRowScanRel.class,
                            new RelOptRuleOperand[] {
                                new RelOptRuleOperand(
                                    LcsIndexIntersectRel.class,
                                    null)
                            })
                    }),
                "with intersect child"));
        planner.addRule(
            new LcsIndexSemiJoinRule(
                new RelOptRuleOperand(
                    SemiJoinRel.class,
                    new RelOptRuleOperand[] {
                        new RelOptRuleOperand(
                            LcsRowScanRel.class,
                            new RelOptRuleOperand[] {
                                new RelOptRuleOperand(
                                    LcsIndexSearchRel.class,
                                    null)
                            })
                    }),
                "with index search child"));
        planner.addRule(
            new LcsIndexSemiJoinRule(
                new RelOptRuleOperand(
                    SemiJoinRel.class,
                    new RelOptRuleOperand[] {
                        new RelOptRuleOperand(
                            LcsRowScanRel.class,
                            new RelOptRuleOperand[] {
                                new RelOptRuleOperand(
                                    LcsIndexMergeRel.class,
                                    new RelOptRuleOperand[] {
                                        new RelOptRuleOperand(
                                            LcsIndexSearchRel.class,
                                            null)
                                    })
                            })
                    }),
                "with merge child"));

        // IndexAccess rules
        planner.addRule(
            new LcsIndexAccessRule(
                new RelOptRuleOperand(
                    FilterRel.class,
                    new RelOptRuleOperand[] {
                        new RelOptRuleOperand(LcsRowScanRel.class, null)
                    }),
                "without index child"));
        planner.addRule(
            new LcsIndexAccessRule(
                new RelOptRuleOperand(
                    FilterRel.class,
                    new RelOptRuleOperand[] {
                        new RelOptRuleOperand(
                            LcsRowScanRel.class,
                            new RelOptRuleOperand[] {
                                new RelOptRuleOperand(
                                    LcsIndexIntersectRel.class,
                                    null)
                            })
                    }),
                "with intersect child"));
        planner.addRule(
            new LcsIndexAccessRule(
                new RelOptRuleOperand(
                    FilterRel.class,
                    new RelOptRuleOperand[] {
                        new RelOptRuleOperand(
                            LcsRowScanRel.class,
                            new RelOptRuleOperand[] {
                                new RelOptRuleOperand(
                                    LcsIndexSearchRel.class,
                                    null)
                            })
                    }),
                "with index search child"));
        planner.addRule(
            new LcsIndexAccessRule(
                new RelOptRuleOperand(
                    FilterRel.class,
                    new RelOptRuleOperand[] {
                        new RelOptRuleOperand(
                            LcsRowScanRel.class,
                            new RelOptRuleOperand[] {
                                new RelOptRuleOperand(
                                    LcsIndexMergeRel.class,
                                    new RelOptRuleOperand[] {
                                        new RelOptRuleOperand(
                                            LcsIndexSearchRel.class,
                                            null)
                                    })
                            })
                    }),
                "with merge child"));
    }

    // implement FarragoMedLocalDataServer
    public void validateTableDefinition(
        FemLocalTable table,
        FemLocalIndex generatedPrimaryKeyIndex,
        boolean creation)
        throws SQLException
    {
        // Verify that no column has a collection for its type, because
        // we don't support those...yet.
        Set<CwmColumn> uncoveredColumns =
            new HashSet<CwmColumn>(
                Util.cast(table.getFeature(), CwmColumn.class));
        for (CwmColumn col : uncoveredColumns) {
            if (col.getType() instanceof FemSqlcollectionType) {
                throw Util.needToImplement(
                    "column-store for collection type");
            }
        }

        // Verify that clustered indexes do not overlap
        for (
            FemLocalIndex index
            : FarragoCatalogUtil.getTableIndexes(repos, table))
        {
            if (!index.isClustered()) {
                continue;
            }

            // LCS clustered indexes are sorted on RID, not value
            index.setSorted(false);
            for (CwmIndexedFeature indexedFeature : index.getIndexedFeature()) {
                if (!uncoveredColumns.contains(indexedFeature.getFeature())) {
                    throw FarragoResource.instance()
                    .ValidatorMultipleClusterForColumn.ex(
                        repos.getLocalizedObjectName(
                            indexedFeature.getFeature()));
                }
                uncoveredColumns.remove(indexedFeature.getFeature());
            }
        }

        // Create system-defined clustered indexes for any columns which aren't
        // covered by user-defined clustered indexes.
        for (CwmColumn col : uncoveredColumns) {
            createSystemIndex(
                "SYS$CLUSTERED_INDEX",
                table,
                col,
                true,
                false,
                false);
        }

        // create the deletion bitmap index if not already created
        if (FarragoCatalogUtil.getDeletionIndex(repos, table) == null) {
            createSystemIndex(
                "SYS$DELETION_INDEX",
                table,
                null,
                false,
                true,
                true);
        }

        // initialize rowcounts
        if (creation) {
            FarragoCatalogUtil.resetRowCounts((FemAbstractColumnSet) table);
        }
    }

    /**
     * Creates an index with an internally generated name
     *
     * @param namePrefix the initial prefix of the internal name
     * @param table table that the index will be created on
     * @param col column associated with the index; null if the index is not
     * associated with any column
     * @param clustered whether the index is clustered
     * @param sorted whether the index maintains its data in sort order
     * @param unique whether the data in the index is unique
     */
    private void createSystemIndex(
        String namePrefix,
        CwmTable table,
        CwmColumn col,
        boolean clustered,
        boolean sorted,
        boolean unique)
    {
        FemLocalIndex index = repos.newFemLocalIndex();
        String name = namePrefix + "$" + table.getName();
        if (col != null) {
            name = name + "$" + col.getName();
        }
        index.setName(
            FarragoCatalogUtil.uniquifyGeneratedName(repos, col, name));
        index.setSpannedClass(table);
        index.setClustered(clustered);
        index.setSorted(sorted);
        index.setUnique(unique);
        if (col != null) {
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

        LcsIndexGuide indexGuide =
            new LcsIndexGuide(
                (FarragoTypeFactoryImpl) cluster.getTypeFactory(),
                FarragoCatalogUtil.getIndexTable(index),
                index);

        //
        // Unclustered Lcs indexes are implemented as bitmap indexes.
        // To construct bitmap indexes, we pass initiation parameters
        // to the generators as a row of data.
        //
        OneRowRel oneRowRel = new OneRowRel(cluster);
        RexNode [] inputValues =
            indexGuide.getUnclusteredInputs(cluster.getRexBuilder());
        RelDataType rowType = indexGuide.getUnclusteredInputType();
        ProjectRel generatorInputs =
            new ProjectRel(
                cluster,
                oneRowRel,
                inputValues,
                rowType,
                ProjectRel.Flags.Boxed,
                RelCollation.emptyList);

        return new FarragoIndexBuilderRel(
            cluster,
            table,
            generatorInputs,
            index);
    }

    protected void prepareIndexCmd(
        FemIndexCmd cmd,
        FemLocalIndex index)
    {
        // FIXME: should not create new type factory
        LcsIndexGuide indexGuide =
            new LcsIndexGuide(
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

    // implement MedAbstractFennelDataServer
    protected boolean getIncludeTuples(
        FemLocalIndex index)
    {
        return index.isClustered();
    }
}

// End LcsDataServer.java

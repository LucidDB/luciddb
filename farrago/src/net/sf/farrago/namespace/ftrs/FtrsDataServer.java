/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2003-2007 John V. Sichi
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
package net.sf.farrago.namespace.ftrs;

import java.sql.*;

import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.keysindexes.*;
import net.sf.farrago.cwm.relational.enumerations.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.impl.*;
import net.sf.farrago.query.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.type.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;


/**
 * FtrsDataServer implements the {@link FarragoMedDataServer} interface for FTRS
 * data.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FtrsDataServer
    extends MedAbstractFennelDataServer
{
    //~ Instance fields --------------------------------------------------------

    private FarragoTypeFactory indexTypeFactory;

    //~ Constructors -----------------------------------------------------------

    FtrsDataServer(
        String serverMofId,
        Properties props,
        FarragoRepos repos)
    {
        super(serverMofId, props, repos);
        indexTypeFactory = new FarragoTypeFactoryImpl(repos);
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
        return new FtrsTable(localName, rowType, tableProps, columnPropMap);
    }

    // implement FarragoMedDataServer
    public void registerRules(RelOptPlanner planner)
    {
        super.registerRules(planner);
        planner.addRule(new FtrsTableProjectionRule());
        planner.addRule(new FtrsTableModificationRule());
        planner.addRule(new FtrsScanToSearchRule());
        planner.addRule(new FtrsIndexJoinRule());
        planner.addRule(new FtrsRemoveRedundantSortRule());
        planner.addRule(new FtrsIndexBuilderRule());
    }

    // implement FarragoMedLocalDataServer
    public void validateTableDefinition(
        FemLocalTable table,
        FemLocalIndex generatedPrimaryKeyIndex)
        throws SQLException
    {
        // Validate that there's at most one clustered index.
        int nClustered = 0;
        for (
            FemLocalIndex index
            : FarragoCatalogUtil.getTableIndexes(repos, table))
        {
            if (index.isClustered()) {
                nClustered++;
            }
        }
        if (nClustered > 1) {
            throw FarragoResource.instance().ValidatorDuplicateClusteredIndex
            .ex(
                repos.getLocalizedObjectName(table));
        }

        if (FarragoCatalogUtil.getPrimaryKey(table) == null) {
            // TODO:  This is not SQL-standard.  Fixing it requires the
            // introduction of a system-managed surrogate key.
            throw FarragoResource.instance().ValidatorNoPrimaryKey.ex(
                repos.getLocalizedObjectName(table));
        }

        if (generatedPrimaryKeyIndex != null) {
            if (nClustered == 0) {
                // If no clustered index was specified, make the primary
                // key's index clustered.
                generatedPrimaryKeyIndex.setClustered(true);
            }
        }

        // Mark all columns in the clustered index as NOT NULL.
        FemLocalIndex clusteredIndex =
            FarragoCatalogUtil.getClusteredIndex(
                repos,
                table);
        assert (clusteredIndex != null);
        for (
            CwmIndexedFeature indexedFeature
            : clusteredIndex.getIndexedFeature())
        {
            FemStoredColumn col = (FemStoredColumn) indexedFeature.getFeature();
            col.setIsNullable(NullableTypeEnum.COLUMN_NO_NULLS);
        }
    }

    // implement FarragoMedLocalDataServer
    public RelNode constructIndexBuildPlan(
        RelOptTable table,
        FemLocalIndex index,
        RelOptCluster cluster)
    {
        // Construct the equivalent of
        //     SELECT index-coverage-tuple
        //     FROM table
        //     ORDER BY index-coverage-tuple

        FtrsTable ftrsTable = (FtrsTable) table;
        FtrsIndexGuide indexGuide = ftrsTable.getIndexGuide();

        RelNode tableScan = new TableAccessRel(cluster, ftrsTable, null);

        Integer [] projOrdinals = indexGuide.getUnclusteredCoverageArray(index);
        RexNode [] projExps = new RexNode[projOrdinals.length];
        RelDataTypeField [] fields =
            indexGuide.getFlattenedRowType().getFields();
        RelFieldCollation [] collations =
            new RelFieldCollation[projOrdinals.length];
        for (int i = 0; i < projOrdinals.length; ++i) {
            projExps[i] =
                new RexInputRef(
                    projOrdinals[i],
                    fields[projOrdinals[i]].getType());
            collations[i] = new RelFieldCollation(i);
        }

        RelNode project = CalcRel.createProject(tableScan, projExps, null);

        SortRel sort =
            new SortRel(
                cluster,
                project,
                collations);

        return new FarragoIndexBuilderRel(cluster, table, sort, index);
    }

    // implement MedAbstractFennelDataServer
    protected void prepareIndexCmd(
        FemIndexCmd cmd,
        FemLocalIndex index)
    {
        FtrsIndexGuide indexGuide =
            new FtrsIndexGuide(
                indexTypeFactory,
                FarragoCatalogUtil.getIndexTable(index));
        cmd.setTupleDesc(
            indexGuide.getCoverageTupleDescriptor(index));
        cmd.setKeyProj(indexGuide.getDistinctKeyProjection(index));
    }

    // implement MedAbstractFennelDataServer
    protected boolean getIncludeTuples(
        FemLocalIndex index)
    {
        return false;
    }
}

// End FtrsDataServer.java

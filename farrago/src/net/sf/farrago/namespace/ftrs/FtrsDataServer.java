/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 2003-2009 John V. Sichi
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
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.impl.*;
import net.sf.farrago.query.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.type.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
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
        planner.addRule(FtrsTableProjectionRule.instance);
        planner.addRule(FtrsTableModificationRule.instance);
        planner.addRule(FtrsScanToSearchRule.instance);
        planner.addRule(FtrsIndexJoinRule.instance);
        planner.addRule(FtrsRemoveRedundantSortRule.instance);
        planner.addRule(FtrsIndexBuilderRule.instance);
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

    // implement FarragoMedDataServer
    public void registerRelMetadataProviders(ChainedRelMetadataProvider chain)
    {
        super.registerRelMetadataProviders(chain);
        RelMetadataProvider ftrsProvider = new FtrsRelMetadataProvider(repos);
        chain.addProvider(ftrsProvider);
    }

    // implement FarragoMedLocalDataServer
    public boolean supportsAlterTableAddColumn()
    {
        return true;
    }

    //~ Inner Classes ----------------------------------------------------------

    // REVIEW jvs 24-Aug-2008:  do stuff for FtrsIndexSearchRel too?

    public static class FtrsRelMetadataProvider
        extends ReflectiveRelMetadataProvider
    {
        private final FarragoRepos repos;

        private final FtrsColumnMetadata columnMd;

        FtrsRelMetadataProvider(FarragoRepos repos)
        {
            this.repos = repos;
            columnMd = new FtrsColumnMetadata();

            List<Class> args = new ArrayList<Class>();
            args.add((Class) BitSet.class);
            args.add((Class) RexNode.class);
            mapParameterTypes("getDistinctRowCount", args);

            mapParameterTypes(
                "getPopulationSize",
                Collections.singletonList((Class) BitSet.class));

            mapParameterTypes(
                "getUniqueKeys",
                Collections.singletonList((Class) Boolean.TYPE));

            args = new ArrayList<Class>();
            args.add((Class) BitSet.class);
            args.add((Class) Boolean.TYPE);
            mapParameterTypes("areColumnsUnique", args);
        }

        public Double getDistinctRowCount(
            FtrsIndexScanRel rel,
            BitSet groupKey,
            RexNode predicate)
        {
            return columnMd.getDistinctRowCount(rel, groupKey, predicate);
        }

        public Double getPopulationSize(FtrsIndexScanRel rel, BitSet groupKey)
        {
            return columnMd.getPopulationSize(rel, groupKey);
        }

        public Set<BitSet> getUniqueKeys(
            FtrsIndexScanRel rel,
            boolean ignoreNulls)
        {
            return columnMd.getUniqueKeys(rel, repos, ignoreNulls);
        }

        public Boolean areColumnsUnique(
            FtrsIndexScanRel rel,
            BitSet columns,
            boolean ignoreNulls)
        {
            return columnMd.areColumnsUnique(rel, columns, repos, ignoreNulls);
        }
    }

    private static class FtrsColumnMetadata
        extends MedAbstractColumnMetadata
    {
        // implement MedAbstractColumnMetadata
        protected int mapColumnToField(
            RelNode rel,
            FemAbstractColumn keyCol)
        {
            FtrsIndexScanRel scanRel = (FtrsIndexScanRel) rel;
            int origColOrdinal = keyCol.getOrdinal();

            // TODO zfong 5/29/06 - The code below does not account for UDTs.
            // origColOrdinal represents an unflattened column ordinal.  It
            // needs to be converted to a flattened ordinal.  Furthermore, the
            // flattened ordinal may map to multiple fields.
            if (scanRel.projectedColumns == null) {
                return origColOrdinal;
            }
            for (int i = 0; i < scanRel.projectedColumns.length; i++) {
                if (scanRel.projectedColumns[i] == origColOrdinal) {
                    return i;
                }
            }
            return -1;
        }

        // implement MedAbstractColumnMetadata
        protected int mapFieldToColumnOrdinal(RelNode rel, int fieldNo)
        {
            FemAbstractColumn col = mapFieldToColumn(rel, fieldNo);
            if (col == null) {
                return -1;
            }
            return col.getOrdinal();
        }

        // implement MedAbstractColumnMetadata
        protected FemAbstractColumn mapFieldToColumn(RelNode rel, int fieldNo)
        {
            FtrsIndexScanRel scanRel = (FtrsIndexScanRel) rel;
            assert fieldNo >= 0;

            if (scanRel.projectedColumns != null) {
                fieldNo = scanRel.projectedColumns[fieldNo];
            }

            int columnOrdinal =
                scanRel.getIndexGuide().unFlattenOrdinal(fieldNo);
            return (FemAbstractColumn) scanRel.ftrsTable.getCwmColumnSet()
                .getFeature().get(columnOrdinal);
        }
    }
}

// End FtrsDataServer.java

/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.session.*;
import net.sf.farrago.query.*;
import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.impl.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.type.*;
import net.sf.farrago.util.*;

import org.eigenbase.rex.*;
import org.eigenbase.rel.*;
import org.eigenbase.rel.convert.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.util.*;


/**
 * FtrsDataServer implements the {@link FarragoMedDataServer} interface
 * for FTRS data.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FtrsDataServer extends MedAbstractFennelDataServer
{
    //~ Instance fields -------------------------------------------------------

    private FarragoTypeFactory indexTypeFactory;

    //~ Constructors ----------------------------------------------------------

    FtrsDataServer(
        String serverMofId,
        Properties props,
        FarragoRepos repos)
    {
        super(serverMofId, props, repos);
        indexTypeFactory = new FarragoTypeFactoryImpl(repos);
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
        Collection indexes = FarragoCatalogUtil.getTableIndexes(repos, table);
        Iterator indexIter = indexes.iterator();
        int nClustered = 0;
        while (indexIter.hasNext()) {
            FemLocalIndex index = (FemLocalIndex) indexIter.next();
            if (index.isClustered()) {
                nClustered++;
            }
        }
        if (nClustered > 1) {
            throw FarragoResource.instance().
                ValidatorDuplicateClusteredIndex.ex(
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
            projExps[i] = new RexInputRef(
                projOrdinals[i],
                fields[projOrdinals[i]].getType());
            collations[i] = new RelFieldCollation(i);
        }

        ProjectRel project = ProjectRel.create(tableScan, projExps, null);

        SortRel sort =
            new SortRel(
                cluster,
                project,
                collations);
        
        return new FarragoIndexBuilderRel(cluster, sort, index);
    }
    
    // implement MedAbstractFennelDataServer
    protected void prepareIndexCmd(
        FemIndexCmd cmd,
        FemLocalIndex index)
    {
        FtrsIndexGuide indexGuide = new FtrsIndexGuide(
            indexTypeFactory,
            FarragoCatalogUtil.getIndexTable(index));
        cmd.setTupleDesc(
            indexGuide.getCoverageTupleDescriptor(index));
        cmd.setKeyProj(indexGuide.getDistinctKeyProjection(index));
    }
}


// End FtrsDataServer.java

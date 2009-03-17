/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2004-2007 John V. Sichi
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
package net.sf.farrago.namespace.mock;

import java.sql.*;

import java.util.*;

import net.sf.farrago.fem.med.*;
import net.sf.farrago.fennel.*;
import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.impl.*;
import net.sf.farrago.type.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;


/**
 * MedMockLocalDataServer provides a mock implementation of the {@link
 * FarragoMedLocalDataServer} interface.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class MedMockLocalDataServer
    extends MedMockDataServer
    implements FarragoMedLocalDataServer
{
    //~ Constructors -----------------------------------------------------------

    MedMockLocalDataServer(
        MedAbstractDataWrapper wrapper,
        String serverMofId,
        Properties props)
    {
        super(wrapper, serverMofId, props);
    }

    //~ Methods ----------------------------------------------------------------

    // implement FarragoMedLocalDataServer
    public void setFennelDbHandle(FennelDbHandle fennelDbHandle)
    {
        // ignore
    }

    // implement FarragoMedLocalDataServer
    public void validateTableDefinition(
        FemLocalTable table,
        FemLocalIndex generatedPrimaryKeyIndex)
        throws SQLException
    {
        // by default, no special validation rules
    }

    // implement FarragoMedLocalDataServer
    public void validateTableDefinition(
        FemLocalTable table,
        FemLocalIndex generatedPrimaryKeyIndex,
        boolean creation)
        throws SQLException
    {
        validateTableDefinition(table, generatedPrimaryKeyIndex);
    }

    // implement FarragoMedLocalDataServer
    public long createIndex(FemLocalIndex index, FennelTxnContext txnContext)
        throws SQLException
    {
        // mock roots are meaningless
        return 0;
    }

    // implement FarragoMedLocalDataServer
    public void dropIndex(
        FemLocalIndex index,
        long rootPageId,
        boolean truncate,
        FennelTxnContext txnContext)
        throws SQLException
    {
        // ignore
    }

    // implement FarragoMedLocalDataServer
    public FarragoMedLocalIndexStats computeIndexStats(
        FemLocalIndex index,
        long rootPageId,
        boolean estimate,
        FennelTxnContext txnContext)
        throws SQLException
    {
        return new FarragoMedLocalIndexStats(0, -1);
    }

    // implement FarragoMedDataServer
    public void registerRules(RelOptPlanner planner)
    {
        super.registerRules(planner);
        planner.addRule(new MedMockTableModificationRule());
    }

    // override MedMockDataServer
    public FarragoMedColumnSet newColumnSet(
        String [] localName,
        Properties tableProps,
        FarragoTypeFactory typeFactory,
        RelDataType rowType,
        Map<String, Properties> columnPropMap)
        throws SQLException
    {
        long nRows = getLongProperty(tableProps, PROP_ROW_COUNT, 0);
        String executorImpl =
            tableProps.getProperty(PROP_EXECUTOR_IMPL, PROPVAL_JAVA);
        assert (executorImpl.equals(PROPVAL_JAVA)
            || executorImpl.equals(PROPVAL_FENNEL));
        String udxSpecificName = tableProps.getProperty(PROP_UDX_SPECIFIC_NAME);

        if (udxSpecificName != null) {
            assert (executorImpl.equals(PROPVAL_JAVA));
        }

        return new MedMockColumnSet(
            this,
            localName,
            rowType,
            nRows,
            executorImpl,
            udxSpecificName);
    }

    // implement FarragoMedLocalDataServer
    public RelNode constructIndexBuildPlan(
        RelOptTable table,
        FemLocalIndex index,
        RelOptCluster cluster)
    {
        // Fake out the build plan with a dummy iterator which returns a
        // rowcount of 0.
        MedMockIterRel rel =
            new MedMockIterRel(
                new MedMockColumnSet(
                    this,
                    table.getQualifiedName(),
                    RelOptUtil.createDmlRowType(
                        cluster.getTypeFactory()),
                    1,
                    "JAVA",
                    null),
                cluster,
                null);

        // Add a dummy project on top to keep the optimizer happy.
        return RelOptUtil.createRenameRel(
            rel.getRowType(),
            rel);
    }

    //  implement FarragoMedLocalDataServer
    public void versionIndexRoot(
        Long oldRoot,
        Long newRoot,
        FennelTxnContext txnContext)
        throws SQLException
    {
        return;
    }

    // implement FarragoMedLocalDataServer
    public boolean supportsAlterTableAddColumn()
    {
        return false;
    }
}

// End MedMockLocalDataServer.java

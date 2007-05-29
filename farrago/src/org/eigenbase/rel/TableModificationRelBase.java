/*
// $Id$
// Package org.eigenbase is a class library of data management components.
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
package org.eigenbase.rel;

import java.util.*;

import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.sql.type.*;


/**
 * <code>TableModificationRelBase</code> is an abstract base class for
 * implementations of {@link TableModificationRel}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class TableModificationRelBase
    extends SingleRel
{

    //~ Instance fields --------------------------------------------------------

    /**
     * The connection to the optimizing session.
     */
    protected RelOptConnection connection;

    /**
     * The table definition.
     */
    protected final RelOptTable table;
    private final Operation operation;
    private final List<String> updateColumnList;
    private RelDataType inputRowType;
    private final boolean flattened;

    //~ Constructors -----------------------------------------------------------

    protected TableModificationRelBase(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelOptTable table,
        RelOptConnection connection,
        RelNode child,
        Operation operation,
        List<String> updateColumnList,
        boolean flattened)
    {
        super(cluster, traits, child);
        this.table = table;
        this.connection = connection;
        this.operation = operation;
        this.updateColumnList = updateColumnList;
        if (table.getRelOptSchema() != null) {
            cluster.getPlanner().registerSchema(table.getRelOptSchema());
        }
        this.flattened = flattened;
    }

    //~ Methods ----------------------------------------------------------------

    public RelOptConnection getConnection()
    {
        return connection;
    }

    public RelOptTable getTable()
    {
        return table;
    }

    public List<String> getUpdateColumnList()
    {
        return updateColumnList;
    }

    public boolean isFlattened()
    {
        return flattened;
    }

    public Operation getOperation()
    {
        return operation;
    }

    public boolean isInsert()
    {
        return operation == Operation.INSERT;
    }

    public boolean isUpdate()
    {
        return operation == Operation.UPDATE;
    }

    public boolean isDelete()
    {
        return operation == Operation.DELETE;
    }

    public boolean isMerge()
    {
        return operation == Operation.MERGE;
    }

    // implement RelNode
    public RelDataType deriveRowType()
    {
        return RelOptUtil.createDmlRowType(getCluster().getTypeFactory());
    }

    // override RelNode
    public RelDataType getExpectedInputRowType(int ordinalInParent)
    {
        assert (ordinalInParent == 0);

        if (inputRowType != null) {
            return inputRowType;
        }

        if (isUpdate()) {
            inputRowType =
                getCluster().getTypeFactory().createJoinType(
                    new RelDataType[] {
                        table.getRowType(),
                    RelOptUtil.createTypeFromProjection(
                            table.getRowType(),
                            getCluster().getTypeFactory(),
                            updateColumnList)
                    });
        } else if (isMerge()) {
            inputRowType =
                getCluster().getTypeFactory().createJoinType(
                    new RelDataType[] {
                        getCluster().getTypeFactory().createJoinType(
                            new RelDataType[] {
                                table.getRowType(),
                        table.getRowType()
                            }),
                    RelOptUtil.createTypeFromProjection(
                            table.getRowType(),
                            getCluster().getTypeFactory(),
                            updateColumnList)
                    });
        } else {
            inputRowType = table.getRowType();
        }

        if (flattened) {
            inputRowType =
                SqlTypeUtil.flattenRecordType(
                    getCluster().getTypeFactory(),
                    inputRowType,
                    null);
        }

        return inputRowType;
    }

    public void explain(RelOptPlanWriter pw)
    {
        pw.explain(
            this,
            new String[] {
                "child", "table", "operation", "updateColumnList",
                "flattened"
            },
            new Object[] {
                Arrays.asList(table.getQualifiedName()), getOperation(),
                (updateColumnList == null) ? Collections.EMPTY_LIST
                    : updateColumnList,
                flattened
            });
    }

    // implement RelNode
    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        // REVIEW jvs 21-Apr-2006:  Just for now...
        double rowCount = RelMetadataQuery.getRowCount(this);
        return planner.makeCost(rowCount, 0, 0);
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * Enumeration of supported modification operations.
     */
    public enum Operation
    {
        INSERT, UPDATE, DELETE, MERGE;
    }
}

// End TableModificationRelBase.java

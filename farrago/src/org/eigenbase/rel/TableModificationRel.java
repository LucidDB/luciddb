/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
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

package org.eigenbase.rel;

import java.util.*;

import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptConnection;
import org.eigenbase.relopt.RelOptPlanWriter;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.EnumeratedValues;


/**
 * TableModificationRel is like TableAccessRel, but represents a request to
 * modify a table rather than read from it.  It takes one child which produces
 * the modified rows.  (For INSERT, the new values; for DELETE, the old values;
 * for UPDATE, all old values plus updated new values.)
 *
 * @version $Id$
 */
public class TableModificationRel extends SingleRel
{
    //~ Instance fields -------------------------------------------------------

    /** The connection to the optimizing session. */
    protected RelOptConnection connection;

    /** The table definition. */
    protected RelOptTable table;
    private Operation operation;
    private List updateColumnList;
    private RelDataType inputRowType;
    private boolean flattened;

    //~ Constructors ----------------------------------------------------------

    public TableModificationRel(
        RelOptCluster cluster,
        RelOptTable table,
        RelOptConnection connection,
        RelNode child,
        Operation operation,
        List updateColumnList,
        boolean flattened)
    {
        super(cluster, child);
        this.table = table;
        this.connection = connection;
        this.operation = operation;
        this.updateColumnList = updateColumnList;
        if (table.getRelOptSchema() != null) {
            cluster.getPlanner().registerSchema(table.getRelOptSchema());
        }
        this.flattened = flattened;
    }

    //~ Methods ---------------------------------------------------------------

    public RelOptConnection getConnection()
    {
        return connection;
    }

    public RelOptTable getTable()
    {
        return table;
    }

    public List getUpdateColumnList()
    {
        return updateColumnList;
    }

    public boolean isFlattened()
    {
        return flattened;
    }

    // implement Cloneable
    public Object clone()
    {
        return new TableModificationRel(
            cluster,
            table,
            connection,
            RelOptUtil.clone(child),
            operation,
            updateColumnList,
            flattened);
    }

    public Operation getOperation()
    {
        return operation;
    }

    // implement RelNode
    public RelDataType deriveRowType()
    {
        RelDataType [] types = new RelDataType[1];
        String [] fieldNames = new String[1];
        types[0] = cluster.typeFactory.createSqlType(SqlTypeName.Bigint);
        fieldNames[0] = "ROWCOUNT";
        return cluster.typeFactory.createStructType(types, fieldNames);
    }

    // override RelNode
    public RelDataType getExpectedInputRowType(int ordinalInParent)
    {
        assert (ordinalInParent == 0);

        if (inputRowType != null) {
            return inputRowType;
        }

        if (operation.equals(Operation.UPDATE)) {
            inputRowType =
                cluster.typeFactory.createJoinType(
                    new RelDataType [] {
                        table.getRowType(),
                        RelOptUtil.createTypeFromProjection(
                            table.getRowType(),
                            getCluster().typeFactory, 
                            updateColumnList)
                    });
        } else {
            inputRowType = table.getRowType();
        }

        if (flattened) {
            inputRowType = SqlTypeUtil.flattenRecordType(
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
            new String [] { "child", "table", "operation", "updateColumnList",
                            "flattened" },
            new Object [] {
                Arrays.asList(table.getQualifiedName()), getOperation(),
                (updateColumnList == null) ? Collections.EMPTY_LIST
                : updateColumnList,
                Boolean.valueOf(flattened)
            });
    }

    //~ Inner Classes ---------------------------------------------------------

    /**
     * Enumeration of supported modification operations.
     */
    public static class Operation extends EnumeratedValues.BasicValue
    {
        public static final int INSERT_ORDINAL = 1;
        public static final Operation INSERT =
            new Operation("INSERT", INSERT_ORDINAL);
        public static final int UPDATE_ORDINAL = 2;
        public static final Operation UPDATE =
            new Operation("UPDATE", UPDATE_ORDINAL);
        public static final int DELETE_ORDINAL = 3;
        public static final Operation DELETE =
            new Operation("DELETE", DELETE_ORDINAL);

        /**
         * List of all allowable {@link TableModificationRel.Operation} values.
         */
        public static final EnumeratedValues enumeration =
            new EnumeratedValues(new Operation [] { INSERT, UPDATE, DELETE });

        private Operation(
            String name,
            int ordinal)
        {
            super(name, ordinal, null);
        }

        /**
         * Looks up a operation from its ordinal.
         */
        public static Operation get(int ordinal)
        {
            return (Operation) enumeration.getValue(ordinal);
        }

        /**
         * Looks up an operation from its name.
         */
        public static Operation get(String name)
        {
            return (Operation) enumeration.getValue(name);
        }
    }
}


// End TableModificationRel.java

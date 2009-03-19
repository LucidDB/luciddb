/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 SQLstream, Inc.
// Copyright (C) 2005-2007 LucidEra, Inc.
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

import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;


/**
 * <code>TableAccessRelBase</code> is an abstract base class for implementations
 * of {@link TableAccessRel}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class TableAccessRelBase
    extends AbstractRelNode
{
    //~ Instance fields --------------------------------------------------------

    /**
     * The connection to the optimizing session.
     */
    protected RelOptConnection connection;

    /**
     * The table definition.
     */
    protected RelOptTable table;

    //~ Constructors -----------------------------------------------------------

    protected TableAccessRelBase(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelOptTable table,
        RelOptConnection connection)
    {
        super(cluster, traits);
        this.table = table;
        this.connection = connection;
        if (table.getRelOptSchema() != null) {
            cluster.getPlanner().registerSchema(table.getRelOptSchema());
        }
    }

    //~ Methods ----------------------------------------------------------------

    public RelOptConnection getConnection()
    {
        return connection;
    }

    public double getRows()
    {
        return table.getRowCount();
    }

    public RelOptTable getTable()
    {
        return table;
    }

    public List<RelCollation> getCollationList()
    {
        return table.getCollationList();
    }

    public TableAccessRelBase clone()
    {
        return this;
    }

    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        double dRows = table.getRowCount();
        double dCpu = dRows + 1; // ensure non-zero cost
        double dIo = 0;
        return planner.makeCost(dRows, dCpu, dIo);
    }

    public RelDataType deriveRowType()
    {
        return table.getRowType();
    }

    public void explain(RelOptPlanWriter pw)
    {
        pw.explain(
            this,
            new String[] { "table" },
            new Object[] { Arrays.asList(table.getQualifiedName()) });
    }
}

// End TableAccessRelBase.java

/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
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

package org.eigenbase.rel;

import java.util.Arrays;

import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;


/**
 * A <code>TableAccessRel</code> reads all the rows from a {@link
 * RelOptTable}.
 *
 * <p> If the table is a {@link net.sf.saffron.ext.JdbcTable}, then this is
 * literally possible. But for other kinds of tables, there may be many ways to
 * read the data from the table. For some kinds of table, it may not even be
 * possible to read all of the rows unless some narrowing constraint is
 * applied. In the example of the {@link net.sf.saffron.ext.ReflectSchema}
 * schema,
 *
 * <blockquote>
 * <pre>select from fields</pre>
 * </blockquote>
 * cannot be implemented, but
 * <blockquote>
 * <pre>select from fields as f
 * where f.getClass().getName().equals("java.lang.String")</pre>
 * </blockquote>
 * can. It is the optimizer's responsibility to find these ways, by applying
 * transformation rules.
 * </p>
 *
 * @author jhyde
 * @version $Id$
 *
 * @since 10 November, 2001
 */
public class TableAccessRel extends AbstractRelNode
{
    //~ Instance fields -------------------------------------------------------

    /** The connection to the optimizing session. */
    protected RelOptConnection connection;

    /** The table definition. */
    protected RelOptTable table;

    //~ Constructors ----------------------------------------------------------

    public TableAccessRel(
        RelOptCluster cluster,
        RelOptTable table,
        RelOptConnection connection)
    {
        this(
            cluster, new RelTraitSet(CallingConvention.NONE), table,
            connection);
    }

    protected TableAccessRel(
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

    //~ Methods ---------------------------------------------------------------

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

    public Object clone()
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
            new String [] { "table" },
            new Object [] { Arrays.asList(table.getQualifiedName()) });
    }
}


// End TableAccessRel.java

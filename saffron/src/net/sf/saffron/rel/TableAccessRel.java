/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
// (C) Copyright 2003-2004 John V. Sichi
// You must accept the terms in LICENSE.html to use this software.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

package net.sf.saffron.rel;

import net.sf.saffron.core.*;
import net.sf.saffron.opt.PlanCost;
import net.sf.saffron.opt.VolcanoCluster;

import java.util.Arrays;

/**
 * A <code>TableAccessRel</code> reads all the rows from a {@link
 * SaffronTable}.
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
public class TableAccessRel extends SaffronRel
{
    //~ Instance fields -------------------------------------------------------

    /** The connection to Saffron. */
    protected SaffronConnection connection;
    /** The table definition. */
    protected SaffronTable table;

    //~ Constructors ----------------------------------------------------------

    public TableAccessRel(
        VolcanoCluster cluster,
        SaffronTable table,
        SaffronConnection connection)
    {
        super(cluster);
        this.table = table;
        this.connection = connection;
        if (table.getSaffronSchema() != null) {
            cluster.getPlanner().registerSchema(table.getSaffronSchema());
        }
    }

    //~ Methods ---------------------------------------------------------------

    public SaffronConnection getConnection()
    {
        return connection;
    }

    public double getRows()
    {
        return table.getRowCount();
    }

    public SaffronTable getTable()
    {
        return table;
    }

    public Object clone()
    {
        return this;
    }

    public PlanCost computeSelfCost(SaffronPlanner planner)
    {
        double dRows = table.getRowCount();
        double dCpu = dRows + 1; // ensure non-zero cost
        double dIo = 0;
        return planner.makeCost(dRows,dCpu,dIo);
    }

    public SaffronType deriveRowType()
    {
        return table.getRowType();
    }

    public void explain(PlanWriter pw)
    {
        pw.explain(
            this,
            new String [] { "table" },
            new Object [] { Arrays.asList(table.getQualifiedName()) });
    }
}


// End TableAccessRel.java

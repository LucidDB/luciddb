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

package net.sf.saffron.ext;

import org.eigenbase.relopt.RelOptPlanWriter;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.rel.AbstractRelNode;


/**
 * <code>ExtentRel</code> represents all of the instances of a particular
 * class (including subtypes).
 *
 * <p>
 * It cannot be implemented as such, but can often be transformed into an
 * expression which can. For example, in
 * <blockquote>
 * <pre>JavaReflectConnection reflect;
 * select
 * from reflect.fields as field
 * join reflect.classes as clazz
 * on field.getDeclaringClass() == clazz &&
 *     clazz.getPackage().getName().equals("java.lang");</pre>
 * </blockquote>
 * the <code>field</code> and <code>clazz</code> are constrained by join and
 * filter conditions so that we can enumerate the required fields.
 * </p>
 *
 * <p>
 * todo: Why is this not just a TableAccess?
 * </p>
 *
 * @see ExtentTable
 */
public class ExtentRel extends AbstractRelNode
{
    //~ Instance fields -------------------------------------------------------

    private RelOptTable table;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates an <code>ExtentRel</code>.
     *
     * @pre rowType != null
     * @pre table != null
     */
    public ExtentRel(
        RelOptCluster cluster,
        RelDataType rowType,
        RelOptTable table)
    {
        super(cluster);
        assert(rowType != null);
        assert(table != null);
        this.rowType = rowType;
        this.table = table;
        cluster.getPlanner().registerSchema(table.getRelOptSchema());
    }

    //~ Methods ---------------------------------------------------------------

    public boolean isDistinct()
    {
        return true;
    }

    public Object clone()
    {
        return this;
    }

    public void explain(RelOptPlanWriter pw)
    {
        pw.explain(
            this,
            new String [] { "class" },
            new Object [] { getRowType() });
    }

    public RelOptTable getTable()
    {
        return table;
    }
}


// End ExtentRel.java

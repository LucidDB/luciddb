/*
// $Id$
// Farrago is an extensible data management system.
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
package net.sf.farrago.query;

import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.fem.med.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.sql2rel.*;


/**
 * FarragoIndexBuilderRel is the abstract relational expression corresponding to
 * building an index on a table. It is declared here rather than in {@link
 * org.eigenbase.rel} because indexes are not part of pure relational algebra.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoIndexBuilderRel
    extends SingleRel
    implements RelStructuredTypeFlattener.SelfFlatteningRel
{
    //~ Instance fields --------------------------------------------------------

    /**
     * Index to be built.
     */
    private final FemLocalIndex index;

    /**
     * Table index belongs to
     */
    private final RelOptTable table;

    //~ Constructors -----------------------------------------------------------

    public FarragoIndexBuilderRel(
        RelOptCluster cluster,
        RelOptTable table,
        RelNode child,
        FemLocalIndex index)
    {
        super(
            cluster,
            new RelTraitSet(CallingConvention.NONE),
            child);
        this.table = table;
        this.index = index;
    }

    //~ Methods ----------------------------------------------------------------

    public FemLocalIndex getIndex()
    {
        return index;
    }

    public RelOptTable getTable()
    {
        return table;
    }

    // implement Cloneable
    public FarragoIndexBuilderRel clone()
    {
        FarragoIndexBuilderRel clone =
            new FarragoIndexBuilderRel(
                getCluster(),
                getTable(),
                getChild().clone(),
                index);
        clone.inheritTraitsFrom(this);
        return clone;
    }

    // implement RelNode
    public RelDataType deriveRowType()
    {
        return RelOptUtil.createDmlRowType(
            getCluster().getTypeFactory());
    }

    // implement RelNode
    public void explain(RelOptPlanWriter pw)
    {
        pw.explain(
            this,
            new String[] { "child", "index" },
            new Object[] {
                Arrays.asList(
                    FarragoCatalogUtil.getQualifiedName(index).names)
            });
    }

    // implement RelStructuredTypeFlattener.SelfFlatteningRel
    public void flattenRel(RelStructuredTypeFlattener flattener)
    {
        flattener.rewriteGeneric(this);
    }
}

// End FarragoIndexBuilderRel.java

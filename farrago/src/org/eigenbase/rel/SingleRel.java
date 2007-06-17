/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2002-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2003-2007 John V. Sichi
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

import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.util.*;


/**
 * A <code>SingleRel</code> is a base class single-input relational expressions.
 *
 * @author jhyde
 * @version $Id$
 * @since 23 September, 2001
 */
public abstract class SingleRel
    extends AbstractRelNode
{
    //~ Instance fields --------------------------------------------------------

    private RelNode child;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a <code>SingleRel</code>.
     *
     * @param cluster {@link RelOptCluster}  this relational expression belongs
     * to
     * @param child input relational expression
     */
    protected SingleRel(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode child)
    {
        super(cluster, traits);
        this.child = child;
    }

    //~ Methods ----------------------------------------------------------------

    public RelNode getChild()
    {
        return child;
    }

    // implement RelNode
    public RelNode [] getInputs()
    {
        return new RelNode[] { child };
    }

    public double getRows()
    {
        // Not necessarily correct, but a better default than Rel's 1.0
        return RelMetadataQuery.getRowCount(child);
    }

    public void childrenAccept(RelVisitor visitor)
    {
        visitor.visit(child, 0, this);
    }

    public void explain(RelOptPlanWriter pw)
    {
        pw.explain(
            this,
            new String[] { "child" },
            Util.emptyObjectArray);
    }

    // override Rel
    public void replaceInput(
        int ordinalInParent,
        RelNode rel)
    {
        assert (ordinalInParent == 0);
        this.child = rel;
    }

    protected RelDataType deriveRowType()
    {
        return child.getRowType();
    }
}

// End SingleRel.java

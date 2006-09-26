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

import org.eigenbase.relopt.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;


/**
 * Relational expression which imposes a particular sort order on its input
 * without otherwise changing its content.
 */
public final class SortRel
    extends SingleRel
{

    //~ Instance fields --------------------------------------------------------

    protected final RelFieldCollation [] collations;
    protected final RexNode [] fieldExps;
    protected final Double estimatedNumRows;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a sorter.
     *
     * @param cluster {@link RelOptCluster} this relational expression belongs
     * to
     * @param child input relational expression
     * @param collations array of sort specifications
     */
    public SortRel(
        RelOptCluster cluster,
        RelNode child,
        RelFieldCollation [] collations)
    {
        super(
            cluster,
            new RelTraitSet(CallingConvention.NONE),
            child);
        this.collations = collations;

        fieldExps = new RexNode[collations.length];
        final RelDataTypeField [] fields = getRowType().getFields();
        for (int i = 0; i < collations.length; ++i) {
            int iField = collations[i].getFieldIndex();
            fieldExps[i] =
                cluster.getRexBuilder().makeInputRef(
                    fields[iField].getType(),
                    iField);
        }
        // save the input row count while we still have logical RelNodes
        estimatedNumRows = RelMetadataQuery.getRowCount(child);
    }

    //~ Methods ----------------------------------------------------------------

    public SortRel clone()
    {
        SortRel clone =
            new SortRel(
                getCluster(),
                getChild().clone(),
                collations);
        clone.inheritTraitsFrom(this);
        return clone;
    }

    public RexNode [] getChildExps()
    {
        return fieldExps;
    }

    /**
     * @return array of RelFieldCollations, from most significant to least
     * significant
     */
    public RelFieldCollation [] getCollations()
    {
        return collations;
    }
    
    /**
     * @return estimated number of rows in the sort input
     */
    public Double getEstimatedNumRows()
    {
        return estimatedNumRows;
    }

    public void explain(RelOptPlanWriter pw)
    {
        String [] terms = new String[1 + (collations.length * 2)];
        Object [] values = new Object[collations.length];
        int i = 0;
        terms[i++] = "child";
        for (int j = 0; j < collations.length; ++j) {
            terms[i++] = "sort" + j;
        }
        for (int j = 0; j < collations.length; ++j) {
            terms[i++] = "dir" + j;
            values[j] = collations[j].getDirection();
        }
        pw.explain(this, terms, values);
    }
}

// End SortRel.java

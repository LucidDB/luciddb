/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later Eigenbase-approved version.
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

import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptPlanWriter;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.rex.RexNode;


/**
 * Relational expression which imposes a
 * particular sort order on its input without otherwise changing its content.
 */
public class SortRel extends SingleRel
{
    //~ Instance fields -------------------------------------------------------

    protected final RelFieldCollation [] collations;
    protected final RexNode [] fieldExps;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a sorter.
     *
     * @param cluster {@link RelOptCluster} this relational expression
     *        belongs to
     * @param child input relational expression
     * @param collations array of sort specifications
     */
    public SortRel(
        RelOptCluster cluster,
        RelNode child,
        RelFieldCollation [] collations)
    {
        super(cluster, child);
        this.collations = collations;

        fieldExps = new RexNode[collations.length];
        final RelDataTypeField [] fields = getRowType().getFields();
        for (int i = 0; i < collations.length; ++i) {
            int iField = collations[i].iField;
            fieldExps[i] =
                cluster.rexBuilder.makeInputRef(
                    fields[iField].getType(),
                    iField);
        }
    }

    //~ Methods ---------------------------------------------------------------

    public Object clone()
    {
        return new SortRel(
            cluster,
            RelOptUtil.clone(child),
            collations);
    }

    public RexNode [] getChildExps()
    {
        return fieldExps;
    }

    /**
     * .
     *
     * @return array of RelFieldCollations, from most significant to least
     * significant
     */
    public RelFieldCollation [] getCollations()
    {
        return collations;
    }

    public void explain(RelOptPlanWriter pw)
    {
        int i = 0;
        String [] terms = new String[1 + collations.length];
        terms[i++] = "child";
        for (int j = 0; j < collations.length; ++j) {
            terms[i++] = "sort" + j;
        }
        pw.explain(this, terms);
    }
}


// End SortRel.java

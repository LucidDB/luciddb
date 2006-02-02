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

import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.relopt.CallingConvention;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexUtil;
import org.eigenbase.reltype.RelDataType;


/**
 * <code>ProjectRel</code> is a relational expression which computes a set of
 * 'select expressions' from its input relational expression.
 *
 * <p>
 * The result is usually 'boxed' as a record with one named field for each
 * column; if there is precisely one expression, the result may be 'unboxed',
 * and consist of the raw value type.
 * </p>
 */
public final class ProjectRel extends ProjectRelBase
{
    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a ProjectRel.
     *
     * @param cluster {@link RelOptCluster} this relational expression
     *        belongs to
     * @param child input relational expression
     * @param exps set of expressions for the input columns
     * @param fieldNames aliases of the expressions
     * @param flags values as in {@link ProjectRelBase.Flags}
     */
    public ProjectRel(
        RelOptCluster cluster,
        RelNode child,
        RexNode [] exps,
        String [] fieldNames,
        int flags)
    {
        this(
            cluster, child, exps,
            RexUtil.createStructType(cluster.getTypeFactory(), exps, fieldNames),
            flags);
    }

    /**
     * Creates a ProjectRel.
     *
     * @param cluster {@link RelOptCluster} this relational expression
     *        belongs to
     * @param child input relational expression
     * @param exps set of expressions for the input columns
     * @param rowType output row type
     * @param flags values as in {@link ProjectRelBase.Flags}
     */
    public ProjectRel(
        RelOptCluster cluster,
        RelNode child,
        RexNode [] exps,
        RelDataType rowType,
        int flags)
    {
        super(
            cluster,
            new RelTraitSet(CallingConvention.NONE),
            child, exps, rowType, flags);
    }

    //~ Methods ---------------------------------------------------------------

    public Object clone()
    {
        ProjectRel clone = new ProjectRel(
            getCluster(),
            RelOptUtil.clone(getChild()),
            RexUtil.clone(exps),
            rowType,
            getFlags());
        clone.inheritTraitsFrom(this);
        return clone;
    }
}


// End ProjectRel.java

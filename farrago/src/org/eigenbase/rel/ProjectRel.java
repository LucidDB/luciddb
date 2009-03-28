/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2002-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 2003-2009 John V. Sichi
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
import org.eigenbase.rex.*;
import org.eigenbase.util.*;


/**
 * <code>ProjectRel</code> is a relational expression which computes a set of
 * 'select expressions' from its input relational expression.
 *
 * <p>The result is usually 'boxed' as a record with one named field for each
 * column; if there is precisely one expression, the result may be 'unboxed',
 * and consist of the raw value type.</p>
 *
 * @version $Id$
 * @author jhyde
 * @since March, 2004
 */
public final class ProjectRel
    extends ProjectRelBase
{
    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a ProjectRel with no sort keys.
     *
     * @param cluster Cluster this relational expression belongs to
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
            cluster,
            child,
            exps,
            RexUtil.createStructType(
                cluster.getTypeFactory(),
                exps,
                fieldNames),
            flags,
            Collections.<RelCollation>emptyList());
    }

    /**
     * Creates a ProjectRel.
     *
     * @param cluster Cluster this relational expression belongs to
     * @param child input relational expression
     * @param exps set of expressions for the input columns
     * @param rowType output row type
     * @param flags values as in {@link ProjectRelBase.Flags}
     * @param collationList List of sort keys
     */
    public ProjectRel(
        RelOptCluster cluster,
        RelNode child,
        RexNode [] exps,
        RelDataType rowType,
        int flags,
        final List<RelCollation> collationList)
    {
        super(
            cluster,
            new RelTraitSet(CallingConvention.NONE),
            child,
            exps,
            rowType,
            flags,
            collationList);
    }

    //~ Methods ----------------------------------------------------------------

    public ProjectRel clone()
    {
        ProjectRel clone =
            new ProjectRel(
                getCluster(),
                getChild().clone(),
                RexUtil.clone(exps),
                rowType,
                getFlags(),
                Collections.<RelCollation>emptyList());
        clone.inheritTraitsFrom(this);
        return clone;
    }

    /**
     * Returns a permutation, if this projection is merely a permutation of its
     * input fields, otherwise null.
     */
    public Permutation getPermutation()
    {
        final int fieldCount = rowType.getFields().length;
        if (fieldCount != getChild().getRowType().getFields().length) {
            return null;
        }
        Permutation permutation = new Permutation(fieldCount);
        for (int i = 0; i < fieldCount; ++i) {
            if (exps[i] instanceof RexInputRef) {
                permutation.set(i, ((RexInputRef) exps[i]).getIndex());
            } else {
                return null;
            }
        }
        return permutation;
    }
}

// End ProjectRel.java

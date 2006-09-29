/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2006 The Eigenbase Project
// Copyright (C) 2002-2006 Disruptive Tech
// Copyright (C) 2005-2006 LucidEra, Inc.
// Portions Copyright (C) 2003-2006 John V. Sichi
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
package org.eigenbase.oj.rel;

import java.util.Collections;

import openjava.mop.*;

import openjava.ptree.*;

import org.eigenbase.oj.util.*;
import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.runtime.*;


/**
 * <code>IterOneRowRel</code> is an iterator implementation of {@link
 * OneRowRel}.
 */
public class IterOneRowRel
    extends OneRowRelBase
    implements JavaRel
{

    //~ Constructors -----------------------------------------------------------

    public IterOneRowRel(RelOptCluster cluster)
    {
        super(
            cluster,
            new RelTraitSet(CallingConvention.ITERATOR));
    }

    //~ Methods ----------------------------------------------------------------

    public IterOneRowRel clone()
    {
        return this;
    }

    // implement RelNode
    public ParseTree implement(JavaRelImplementor implementor)
    {
        OJClass outputRowClass =
            OJUtil.typeToOJClass(
                getRowType(),
                getCluster().getTypeFactory());

        Expression newRowExp =
            new AllocationExpression(
                TypeName.forOJClass(outputRowClass),
                new ExpressionList());

        Expression iterExp =
            new AllocationExpression(
                OJUtil.typeNameForClass(
                    RestartableCollectionTupleIter.class),
                new ExpressionList(
                    new MethodCall(
                        OJUtil.typeNameForClass(Collections.class),
                        "singletonList",
                        new ExpressionList(newRowExp))));
        return iterExp;
    }
}

// End IterOneRowRel.java

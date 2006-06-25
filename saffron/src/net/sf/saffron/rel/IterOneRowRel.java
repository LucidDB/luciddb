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

package net.sf.saffron.rel;

import java.util.Collections;

import openjava.mop.OJClass;
import openjava.ptree.*;

import org.eigenbase.util.Util;
import org.eigenbase.oj.util.OJUtil;
import org.eigenbase.oj.rel.JavaRelImplementor;
import org.eigenbase.oj.rel.JavaRel;
import org.eigenbase.rel.*;
import org.eigenbase.relopt.CallingConvention;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.runtime.*;


/**
 * <code>IterOneRowRel</code> is an iterator implementation of
 * {@link OneRowRel}.
 */
public class IterOneRowRel extends OneRowRelBase implements JavaRel
{
    //~ Constructors ----------------------------------------------------------

    public IterOneRowRel(RelOptCluster cluster)
    {
        super(cluster, new RelTraitSet(CallingConvention.ITERATOR));
    }

    //~ Methods ---------------------------------------------------------------

    public Object clone()
    {
        return this;
    }

    // implement RelNode
    public ParseTree implement(JavaRelImplementor implementor)
    {
        OJClass outputRowClass = OJUtil.typeToOJClass(
            getRowType(), getCluster().getTypeFactory());

        Expression newRowExp =
            new AllocationExpression(
                TypeName.forOJClass(outputRowClass),
                new ExpressionList());

        Expression iterExp =
            new AllocationExpression(
                OJUtil.typeNameForClass(RestartableCollectionIterator.class),
                new ExpressionList(
                    new MethodCall(
                        OJUtil.typeNameForClass(Collections.class),
                        "singletonList",
                        new ExpressionList(newRowExp))));

        return iterExp;
    }
}


// End IterOneRowRel.java

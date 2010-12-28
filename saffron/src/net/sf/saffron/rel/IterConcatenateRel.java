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

import openjava.mop.OJClass;
import openjava.ptree.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.CallingConvention;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.oj.util.OJUtil;
import org.eigenbase.oj.rel.JavaRel;
import org.eigenbase.oj.rel.JavaRelImplementor;


/**
 * <code>IterConcatenateRel</code> concatenates several iterators. It is an
 * iterator implementation of {@link UnionRel}.
 */
public class IterConcatenateRel extends UnionRelBase implements JavaRel
{
    //~ Constructors ----------------------------------------------------------

    public IterConcatenateRel(
        RelOptCluster cluster,
        RelNode [] inputs)
    {
        super(
            cluster, new RelTraitSet(CallingConvention.ITERATOR), inputs,
            true /*all*/);
    }

    //~ Methods ---------------------------------------------------------------

    public IterConcatenateRel clone()
    {
        // REVIEW jvs 13-Nov-2005:  shouldn't we be cloning the inputs too?
        IterConcatenateRel clone = new IterConcatenateRel(getCluster(), inputs);
        clone.inheritTraitsFrom(this);
        return clone;
    }

    public SetOpRel clone(RelNode[] inputs, boolean all)
    {
        assert all;
        return new IterConcatenateRel(getCluster(), inputs.clone());
    }

    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        double dRows = getRows();
        // favor a Nexus over a CompoundIterator, due to hassles of java/c++/java data transfer
        double dCpu = 1000;
        double dIo = 1000;
        return planner.makeCost(dRows, dCpu, dIo);
    }

    protected OJClass getCompoundIteratorClass()
    {
        return OJClass.forClass(org.eigenbase.runtime.CompoundIterator.class);
    }

    public ParseTree implement(JavaRelImplementor implementor)
    {
        // Generate
        //   new CompoundIterator(
        //     new Iterator[] {<<input0>>, ...})
        // If any input is infinite, should instead generate
        //   new CompoundParallelIterator(
        //     new Iterator[] {<<input0>>, ...})
        // but there's no way to tell, so we can't.
        // REVIEW: mb 9-Sep-2005: add a predicate RelNode.isInfinite().
        ExpressionList exps = new ExpressionList();
        for (int i = 0; i < inputs.length; i++) {
            Expression exp =
                implementor.visitJavaChild(this, i, (JavaRel) inputs[i]);
            exps.add(exp);
        }
        return new AllocationExpression(
            getCompoundIteratorClass(),
            new ExpressionList(
                new ArrayAllocationExpression(
                    OJUtil.clazzIterator,
                    new ExpressionList(null),
                    new ArrayInitializer(exps))));
    }
}


// End IterConcatenateRel.java

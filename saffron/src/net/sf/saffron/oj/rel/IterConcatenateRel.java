/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
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

package net.sf.saffron.oj.rel;

import net.sf.saffron.core.SaffronPlanner;
import net.sf.saffron.opt.CallingConvention;
import net.sf.saffron.opt.PlanCost;
import net.sf.saffron.opt.RelImplementor;
import net.sf.saffron.opt.VolcanoCluster;
import net.sf.saffron.rel.SaffronRel;
import net.sf.saffron.rel.UnionRel;
import net.sf.saffron.util.Util;

import openjava.mop.OJClass;

import openjava.ptree.*;


/**
 * <code>IterConcatenateRel</code> concatenates several iterators. It is an
 * iterator implementation of {@link UnionRel}.
 */
public class IterConcatenateRel extends UnionRel
{
    //~ Constructors ----------------------------------------------------------

    public IterConcatenateRel(VolcanoCluster cluster,SaffronRel [] inputs)
    {
        super(cluster,inputs,true /*all*/);
    }

    //~ Methods ---------------------------------------------------------------

    public CallingConvention getConvention()
    {
        return CallingConvention.ITERATOR;
    }

    public Object clone()
    {
        return new IterConcatenateRel(cluster,inputs);
    }

    public PlanCost computeSelfCost(SaffronPlanner planner)
    {
        double dRows = getRows();
        double dCpu = 0;
        double dIo = 0;
        return planner.makeCost(dRows,dCpu,dIo);
    }

    public Object implement(RelImplementor implementor,int ordinal)
    {
        switch (ordinal) {
        case -1:

            // Generate
            //   new CompoundIterator(
            //     new Iterator[] {<<input0>>, ...})
            ExpressionList exps = new ExpressionList();
            for (int i = 0; i < inputs.length; i++) {
                Expression exp =
                    (Expression) implementor.implementChild(this,i,inputs[i]);
                exps.add(exp);
            }
            return new AllocationExpression(
                OJClass.forClass(
                    net.sf.saffron.runtime.CompoundIterator.class),
                new ExpressionList(
                    new ArrayAllocationExpression(
                        Util.clazzIterator,
                        new ExpressionList(null),
                        new ArrayInitializer(exps))));
        default:
            throw Util.newInternal("implement: ordinal=" + ordinal);
        }
    }
}


// End IterConcatenateRel.java

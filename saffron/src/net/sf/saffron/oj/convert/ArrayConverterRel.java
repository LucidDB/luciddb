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

package net.sf.saffron.oj.convert;

import net.sf.saffron.core.*;
import net.sf.saffron.oj.util.*;
import net.sf.saffron.opt.*;
import net.sf.saffron.rel.*;
import net.sf.saffron.rel.convert.*;
import net.sf.saffron.runtime.*;
import net.sf.saffron.util.*;

import openjava.mop.*;

import openjava.ptree.*;


/**
 * An <code>ArrayConverterRel</code> converts a plan from
 * <code>inConvention</code> to {@link
 * net.sf.saffron.opt.CallingConvention#ARRAY}.
 */
public class ArrayConverterRel extends ConverterRel
{
    //~ Instance fields -------------------------------------------------------

    Variable var_v;

    //~ Constructors ----------------------------------------------------------

    public ArrayConverterRel(VolcanoCluster cluster,SaffronRel child)
    {
        super(cluster,child);
    }

    //~ Methods ---------------------------------------------------------------

    public CallingConvention getConvention()
    {
        return CallingConvention.ARRAY;
    }

    // implement SaffronRel
    public Object clone()
    {
        return new ArrayConverterRel(cluster,child);
    }

    public static void init(SaffronPlanner planner)
    {
        final ConverterFactory factory =
            new ConverterFactory() {
                public ConverterRel convert(SaffronRel rel)
                {
                    return new ArrayConverterRel(rel.getCluster(),rel);
                }

                public CallingConvention getConvention()
                {
                    return CallingConvention.ARRAY;
                }
            };
        planner.addRule(
            new FactoryConverterRule(factory,CallingConvention.COLLECTION));
        planner.addRule(
            new FactoryConverterRule(factory,CallingConvention.VECTOR));
    }

    public Object implement(RelImplementor implementor,int ordinal)
    {
        switch (inConvention.ordinal_) {
        case CallingConvention.COLLECTION_ORDINAL:
            return implementCollection(implementor,ordinal);
        case CallingConvention.VECTOR_ORDINAL:
            return implementVector(implementor,ordinal);
        default:
            return super.implement(implementor,ordinal);
        }
    }

    private Object implementCollection(RelImplementor implementor,int ordinal)
    {
        switch (ordinal) {
        case -1: // called from parent
            OJClass clazz = OJUtil.typeToOJClass(child.getRowType()); // "Rowtype"
            Expression exp =
                (Expression) implementor.implementChild(this,0,child);
            return clazz.isPrimitive()
            ? new MethodCall(
                TypeName.forOJClass(OJClass.forClass(SaffronUtil.class)),
                "copyInto",
                new ExpressionList(
                    exp,
                    new ArrayAllocationExpression(
                        clazz,
                        new ExpressionList(Literal.constantZero()))))
            : new MethodCall(
                exp,
                "toArray",
                new ExpressionList(
                    new ArrayAllocationExpression(
                        clazz,
                        new ExpressionList(Literal.constantZero()))));
        default:
            throw Util.newInternal("implement: ordinal=" + ordinal);
        }
    }

    private Object implementVector(RelImplementor implementor,int ordinal)
    {
        switch (ordinal) {
        case -1: // called from parent
            OJClass clazz = OJUtil.typeToOJClass(child.getRowType()); // "Rowtype"
            Object o = implementor.implementChild(this,0,child);
            return new MethodCall(
                TypeName.forOJClass(OJClass.forClass(SaffronUtil.class)),
                "copyInto",
                new ExpressionList(
                    new CastExpression(
                        TypeName.forOJClass(Util.clazzVector),
                        (Expression) o),
                    new ArrayAllocationExpression(
                        clazz,
                        new ExpressionList(Literal.constantZero()))));
        default:
            throw Util.newInternal("implement: ordinal=" + ordinal);
        }
    }
}


// End ArrayConverterRel.java

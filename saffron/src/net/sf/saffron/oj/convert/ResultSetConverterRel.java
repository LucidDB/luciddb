/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
// (C) Copyright 2003-2004 John V. Sichi
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
import net.sf.saffron.runtime.IteratorResultSet.*;
import net.sf.saffron.util.*;

import openjava.mop.*;

import openjava.ptree.*;


/**
 * A <code>ResultSetConverterRel</code> converts a plan from
 * <code>inConvention</code> to {@link
 * net.sf.saffron.opt.CallingConvention#RESULT_SET_ORDINAL}.
 */
public class ResultSetConverterRel extends ConverterRel
{
    //~ Constructors ----------------------------------------------------------

    public ResultSetConverterRel(VolcanoCluster cluster,SaffronRel child)
    {
        super(cluster,child);
    }

    //~ Methods ---------------------------------------------------------------

    public CallingConvention getConvention()
    {
        return CallingConvention.RESULT_SET;
    }

    // implement SaffronRel
    public Object clone()
    {
        return new ResultSetConverterRel(cluster,child);
    }

    public static void init(SaffronPlanner planner)
    {
        final ConverterFactory factory =
            new ConverterFactory() {
                public ConverterRel convert(SaffronRel rel)
                {
                    return new ResultSetConverterRel(rel.getCluster(),rel);
                }

                public CallingConvention getConvention()
                {
                    return CallingConvention.RESULT_SET;
                }
            };
        planner.addRule(
            new FactoryConverterRule(factory,CallingConvention.ITERATOR));
    }

    public Object implement(RelImplementor implementor,int ordinal)
    {
        switch (inConvention.ordinal_) {
        case CallingConvention.ITERATOR_ORDINAL:
            return implementIterator(implementor,ordinal);
        default:
            return super.implement(implementor,ordinal);
        }
    }

    private Object implementIterator(RelImplementor implementor,int ordinal)
    {
        switch (ordinal) {
        case -1: // called from parent
            Object o = implementor.implementChild(this,0,child);
            final SaffronType rowType = getRowType();
            OJClass rowClass = OJUtil.typeToOJClass(rowType);
            Expression getter;
            if (true) {
                getter =
                    new AllocationExpression(
                        TypeName.forOJClass(
                            OJClass.forClass(FieldGetter.class)),
                        new ExpressionList(
                            new FieldAccess(
                                TypeName.forOJClass(rowClass),
                                "class")));
            } else if (rowType.isProject() && (rowType.getFieldCount() == 1)) {
                getter =
                    new AllocationExpression(
                        TypeName.forOJClass(
                            OJClass.forClass(SingletonColumnGetter.class)),
                        new ExpressionList());
            } else {
                getter =
                    new AllocationExpression(
                        TypeName.forOJClass(
                            OJClass.forClass(SyntheticColumnGetter.class)),
                        new ExpressionList(
                            new FieldAccess(
                                TypeName.forOJClass(rowClass),
                                "class")));
            }
            return new AllocationExpression(
                TypeName.forOJClass(OJClass.forClass(IteratorResultSet.class)),
                new ExpressionList(
                    new CastExpression(Util.clazzIterator,(Expression) o),
                    getter));
        default:
            throw Util.newInternal("implement: ordinal=" + ordinal);
        }
    }
}


// End ResultSetConverterRel.java

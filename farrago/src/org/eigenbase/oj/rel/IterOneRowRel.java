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

package org.eigenbase.oj.rel;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.oj.util.OJUtil;
import org.eigenbase.relopt.CallingConvention;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.rel.OneRowRel;
import openjava.mop.OJClass;
import openjava.ptree.*;

import java.util.Collections;

/**
 * <code>IterOneRowRel</code> is an iterator implementation of 
 * {@link OneRowRel}.
 */
public class IterOneRowRel extends OneRowRel implements JavaRel
{
    //~ Constructors ----------------------------------------------------------

    public IterOneRowRel(
        RelOptCluster cluster)
    {
        super(cluster);
    }

    //~ Methods ---------------------------------------------------------------

    // implement RelNode
    public CallingConvention getConvention()
    {
        return CallingConvention.ITERATOR;
    }

    public Object clone()
    {
        return this;
    }

    // implement RelNode
    public ParseTree implement(JavaRelImplementor implementor)
    {
        final RelDataType outputRowType = getRowType();
        OJClass outputRowClass = OJUtil.typeToOJClass(outputRowType);

        Expression newRowExp = new AllocationExpression(
            TypeName.forOJClass(outputRowClass),
            new ExpressionList());

        Expression iterExp = new MethodCall(
            new MethodCall(
                TypeName.forClass(Collections.class),
                "singletonList",
                new ExpressionList(newRowExp)),
            "iterator",
            new ExpressionList());
        
        return iterExp;
    }
}

// End IterOneRowRel.java

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

import net.sf.saffron.oj.util.*;
import net.sf.saffron.opt.*;
import net.sf.saffron.rel.*;
import net.sf.saffron.util.*;
import net.sf.saffron.runtime.*;
import net.sf.saffron.core.*;

import java.util.*;

import openjava.ptree.*;
import openjava.mop.*;

/**
 * <code>IterOneRowRel</code> is an iterator implementation of 
 * {@link OneRowRel} and {@link FilterRel}.
 */
public class IterOneRowRel extends OneRowRel
{
    //~ Constructors ----------------------------------------------------------

    public IterOneRowRel(
        VolcanoCluster cluster)
    {
        super(cluster);
    }

    //~ Methods ---------------------------------------------------------------

    // implement SaffronRel
    public CallingConvention getConvention()
    {
        return CallingConvention.ITERATOR;
    }

    public Object clone()
    {
        return this;
    }

    // implement SaffronRel
    public Object implement(RelImplementor implementor,int ordinal)
    {
        if (ordinal != -1) {
            throw Util.newInternal("implement: ordinal=" + ordinal);
        }

        final SaffronType outputRowType = getRowType();
        OJClass outputRowClass = OJUtil.typeToOJClass(outputRowType);

        // REVIEW:  could we just use null instead?
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

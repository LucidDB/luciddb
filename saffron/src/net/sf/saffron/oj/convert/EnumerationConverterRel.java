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
import net.sf.saffron.opt.*;
import net.sf.saffron.rel.*;
import net.sf.saffron.rel.convert.*;
import net.sf.saffron.util.*;

import openjava.ptree.*;


/**
 * <code>EnumerationConverterRel</code> converts a plan from
 * <code>inConvention</code> to {@link
 * net.sf.saffron.opt.CallingConvention#ENUMERATION}.
 *
 * @author jhyde
 * @version $Id$
 *
 * @since 16 December, 2001
 */
public class EnumerationConverterRel extends ConverterRel
{
    //~ Instance fields -------------------------------------------------------

    Variable var_v;

    //~ Constructors ----------------------------------------------------------

    public EnumerationConverterRel(VolcanoCluster cluster,SaffronRel child)
    {
        super(cluster,child);
    }

    //~ Methods ---------------------------------------------------------------

    public CallingConvention getConvention()
    {
        return CallingConvention.ENUMERATION;
    }

    // implement SaffronRel
    public Object clone()
    {
        return new EnumerationConverterRel(cluster,child);
    }

    public static void init(SaffronPlanner planner)
    {
        final ConverterFactory factory =
            new ConverterFactory() {
                public CallingConvention getConvention()
                {
                    return CallingConvention.ENUMERATION;
                }

                public ConverterRel convert(SaffronRel rel)
                {
                    return new EnumerationConverterRel(rel.getCluster(),rel);
                }
            };
        planner.addRule(
            new FactoryConverterRule(factory,CallingConvention.VECTOR));
    }

    public Object implement(RelImplementor implementor,int ordinal)
    {
        switch (inConvention.ordinal_) {
        case CallingConvention.VECTOR_ORDINAL:
            return implementVector(implementor,ordinal);
        default:
            return super.implement(implementor,ordinal);
        }
    }

    private Object implementVector(RelImplementor implementor,int ordinal)
    {
        assert (ordinal == -1) : "Cannot implement callback from child";

        // Generate
        //   <<exp>>.elements()
        Expression exp = (Expression) implementor.implementChild(this,0,child);
        return new MethodCall(exp,"elements",new ExpressionList());
    }
}


// End EnumerationConverterRel.java

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
import net.sf.saffron.opt.CallingConvention;
import net.sf.saffron.opt.OptUtil;
import net.sf.saffron.opt.RelImplementor;
import net.sf.saffron.opt.VolcanoCluster;
import net.sf.saffron.rel.ProjectRel;
import net.sf.saffron.rel.SaffronRel;
import net.sf.saffron.util.Util;
import net.sf.saffron.rex.RexNode;
import net.sf.saffron.rex.RexUtil;

import openjava.ptree.*;


/**
 * Implements the {@link ProjectRel} relational
 * expression as Java code.
 */
public class JavaProjectRel extends ProjectRel implements JavaRel
{
    //~ Constructors ----------------------------------------------------------

    public JavaProjectRel(
        VolcanoCluster cluster,
        SaffronRel child,
        RexNode [] exps,
        String [] fieldNames,
        int flags)
    {
        super(cluster,child,exps,fieldNames,flags);
        assert(child.getConvention() == CallingConvention.JAVA);
    }

    //~ Methods ---------------------------------------------------------------

    // implement SaffronRel
    public CallingConvention getConvention()
    {
        return CallingConvention.JAVA;
    }

    public Object clone()
    {
        return new JavaProjectRel(
            cluster,
            OptUtil.clone(child),
            RexUtil.clone(exps),
            Util.clone(fieldNames),
            getFlags());
    }

    // implement SaffronRel
    public Object implement(RelImplementor implementor,int ordinal)
    {
        switch (ordinal) {
        case -1: // called from parent
            return implementor.implementChild(this,0,child);
        case 0: // called from child
            implementor.generateParentBody(this,null);
            return null;
        default:
            throw Util.newInternal("implement: ordinal=" + ordinal);
        }
    }

    /**
     * Generates the initializer for the variable which holds the current row
     * of this project. If the row-type V is complex, generate
     * <pre><code>
     *  V v = new V(exp, ...);
     *  {parent-handler(v)}
     *  </code></pre>
     * If the row-type is simple, generate
     * <pre><code>
     *  V v = exp;
     *  {parent-handler(v)}
     *  </code></pre>
     * Todo: map relations to expressions (not variables), so we don't have to
     * create new objects:
     * <pre><code>
     *  T1 v1 = exp1;
     *  T2 v2 = exp2;
     *  {parent-handler(v1,v2)}
     *  </code></pre>
     * Or at least, create a holder object outside the loop, and just assign
     * into it:
     * <pre><code>
     *  V v = new V();
     *  {loop} {
     *  v.v1 = exp1;
     *  v.v2 = exp2;
     *  {parent-handler(v)}
     *  }
     *  </code></pre>
     */
    public Expression implementSelf(RelImplementor implementor)
    {
        if (!isBoxed()) {
            // simple row-type, hence "V v = exp;"
            return implementor.translate(this,exps[0]);
        } else {
            // complex row-type, hence "V v = new V(exp, ...);"
            ExpressionList args = implementor.translateList(this,exps);
            return new AllocationExpression(
                OJUtil.toTypeName(getRowType()),
                args);
        }
    }
}


// End JavaProjectRel.java

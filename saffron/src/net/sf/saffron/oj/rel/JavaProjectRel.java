/*
// Saffron preprocessor and data engine.
// Copyright (C) 2002-2004 Disruptive Tech
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
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

package net.sf.saffron.oj.rel;

import openjava.ptree.AllocationExpression;
import openjava.ptree.Expression;
import openjava.ptree.ExpressionList;
import openjava.ptree.ParseTree;

import org.eigenbase.oj.rel.*;
import org.eigenbase.oj.util.OJUtil;
import org.eigenbase.rel.*;
import org.eigenbase.relopt.CallingConvention;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexUtil;
import org.eigenbase.reltype.RelDataType;


/**
 * Implements the {@link ProjectRel} relational
 * expression as Java code.
 */
public class JavaProjectRel
    extends ProjectRelBase
    implements JavaLoopRel, JavaSelfRel
{
    public JavaProjectRel(
        RelOptCluster cluster,
        RelNode child,
        RexNode [] exps,
        RelDataType rowType,
        int flags)
    {
        super(
            cluster, new RelTraitSet(CallingConvention.JAVA), child, exps,
            rowType, flags, RelCollationImpl.createSingleton(0));
        assert (child.getConvention() == CallingConvention.JAVA);
    }

    public JavaProjectRel clone()
    {
        JavaProjectRel clone = new JavaProjectRel(
            getCluster(),
            getChild().clone(),
            RexUtil.clone(exps),
            rowType,
            getFlags());
        clone.inheritTraitsFrom(this);
        return clone;
    }

    // implement RelNode
    public ParseTree implement(JavaRelImplementor implementor)
    {
        return implementor.visitJavaChild(this, 0, (JavaRel) getChild());
    }

    public void implementJavaParent(
        JavaRelImplementor implementor,
        int ordinal)
    {
        assert ordinal == 0;
        implementor.generateParentBody(this, null);
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
    public Expression implementSelf(JavaRelImplementor implementor)
    {
        if (!isBoxed()) {
            // simple row-type, hence "V v = exp;"
            return implementor.translate(this, exps[0]);
        } else {
            // complex row-type, hence "V v = new V(exp, ...);"
            ExpressionList args = implementor.translateList(this, exps);
            return new AllocationExpression(
                OJUtil.toTypeName(
                    getRowType(),
                    implementor.getTypeFactory()),
                args);
        }
    }
}


// End JavaProjectRel.java

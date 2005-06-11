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

import openjava.ptree.Expression;
import openjava.ptree.IfStatement;
import openjava.ptree.ParseTree;
import openjava.ptree.StatementList;

import org.eigenbase.oj.rel.*;
import org.eigenbase.rel.*;
import org.eigenbase.relopt.CallingConvention;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexUtil;
import org.eigenbase.util.Util;


/**
 * Implements the {@link FilterRel} relational expression in Java code.
 */
public class JavaFilterRel extends FilterRelBase implements JavaLoopRel
{
    public JavaFilterRel(
        RelOptCluster cluster,
        RelNode child,
        RexNode condition)
    {
        super(
            cluster, new RelTraitSet(CallingConvention.JAVA), child,
            condition);
    }

    public Object clone()
    {
        JavaFilterRel clone = new JavaFilterRel(
            getCluster(),
            RelOptUtil.clone(getChild()),
            RexUtil.clone(getCondition()));
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
        StatementList stmtList = implementor.getStatementList();
        StatementList ifBody = new StatementList();
        Expression condition2 = implementor.translate(this, getCondition());
        stmtList.add(new IfStatement(condition2, ifBody));
        implementor.bind(this, getChild());
        implementor.generateParentBody(this, ifBody);
    }
}


// End JavaFilterRel.java

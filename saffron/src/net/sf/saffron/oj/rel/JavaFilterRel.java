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

package net.sf.saffron.oj.rel;

import org.eigenbase.oj.rel.*;
import org.eigenbase.relopt.CallingConvention;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.rel.FilterRel;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexUtil;
import org.eigenbase.util.Util;
import openjava.ptree.Expression;
import openjava.ptree.IfStatement;
import openjava.ptree.ParseTree;
import openjava.ptree.StatementList;


/**
 * Implements the {@link FilterRel} relational expression in Java code.
 */
public class JavaFilterRel extends FilterRel implements JavaLoopRel
{
    //~ Constructors ----------------------------------------------------------

    public JavaFilterRel(
        RelOptCluster cluster,
        RelNode child,
        RexNode condition)
    {
        super(cluster,child,condition);
    }

    //~ Methods ---------------------------------------------------------------

    public CallingConvention getConvention()
    {
        return CallingConvention.JAVA;
    }

    public Object clone()
    {
        return new JavaFilterRel(
            cluster,
            RelOptUtil.clone(child),
            RexUtil.clone(condition));
    }

    // implement RelNode
    public ParseTree implement(JavaRelImplementor implementor)
    {
        return implementor.visitJavaChild(this, 0, (JavaRel) child);
    }

    public void implementJavaParent(JavaRelImplementor implementor,
            int ordinal) {
        assert ordinal == 0;
        StatementList stmtList = implementor.getStatementList();
        StatementList ifBody = new StatementList();
        Expression condition2 = implementor.translate(this,condition);
        stmtList.add(new IfStatement(condition2,ifBody));
        implementor.bind(this,child);
        implementor.generateParentBody(this,ifBody);
    }
}


// End JavaFilterRel.java

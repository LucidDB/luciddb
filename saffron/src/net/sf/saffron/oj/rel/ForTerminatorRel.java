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
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.oj.util.OJUtil;
import org.eigenbase.relopt.CallingConvention;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.SingleRel;
import org.eigenbase.util.Util;
import openjava.ptree.*;


/**
 * Converts a {@link RelNode} of
 * {@link CallingConvention#JAVA java calling-convention}
 * into a Java <code>for</code>-loop.
 */
public class ForTerminatorRel extends SingleRel
        implements TerminatorRel, JavaLoopRel
{
    //~ Instance fields -------------------------------------------------------

    StatementList body;
    String label;
    Variable variable;

    //~ Constructors ----------------------------------------------------------

    public ForTerminatorRel(
        RelOptCluster cluster,
        RelNode child,
        Variable variable,
        StatementList body,
        String label)
    {
        super(cluster,child);
        assert(child.getConvention() == CallingConvention.JAVA);
        this.variable = variable;
        this.body = body;
        this.label = label;
    }

    //~ Methods ---------------------------------------------------------------

    // implement RelNode
    public Object clone()
    {
        return new ForTerminatorRel(
            cluster,
            RelOptUtil.clone(child),
            (Variable) Util.clone(variable),
            Util.clone(body),
            label);
    }

    public CallingConvention getConvention()
    {
        return CallingConvention.JAVA;
    }

    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        return planner.makeTinyCost();
    }

    public ParseTree implement(JavaRelImplementor implementor)
    {
        implementor.setExitStatement(new BreakStatement(label));
        StatementList stmtList = new StatementList();
        implementor.pushStatementList(stmtList);
        Object o = implementor.visitJavaChild(this, 0, (JavaRel) child);
        assert(o == null);
        implementor.popStatementList(stmtList);
        return stmtList;
    }

    public void implementJavaParent(JavaRelImplementor implementor, int ordinal) {
        assert ordinal == 0;
        // Generate
        //   Rowtype variable = <<child variable>>;
        //   <<parent body (references variable)>>
        StatementList stmtList = implementor.getStatementList();
        Expression exp = implementor.translateInput(this,0);
        stmtList.add(
            new VariableDeclaration(
                OJUtil.toTypeName(child.getRowType()),
                variable.toString(),
                exp));
        stmtList.addAll(body);
    }
}


// End ForTerminatorRel.java

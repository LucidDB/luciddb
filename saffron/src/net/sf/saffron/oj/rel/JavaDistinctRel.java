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

import openjava.mop.Toolbox;
import openjava.ptree.*;

import org.eigenbase.oj.rel.*;
import org.eigenbase.oj.util.OJUtil;
import org.eigenbase.rel.*;
import org.eigenbase.relopt.CallingConvention;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.util.Util;


/**
 * <code>JavaDistinctRel</code> implements DISTINCT inline. See
 * also {@link JavaAggregateRel}.
 */
public class JavaDistinctRel extends SingleRel implements JavaLoopRel
{
    Variable var_h;

    public JavaDistinctRel(
        RelOptCluster cluster,
        RelNode child)
    {
        super(cluster, new RelTraitSet(CallingConvention.JAVA), child);
    }

    // implement RelNode
    public boolean isDistinct()
    {
        return true;
    }

    // implement RelNode
    public Object clone()
    {
        JavaDistinctRel clone = new JavaDistinctRel(cluster, child);
        clone.traits = cloneTraits();
        return clone;
    }

    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        double dRows = child.getRows();
        double dCpu = Util.nLogN(dRows);
        double dIo = 0;
        return planner.makeCost(dRows, dCpu, dIo);
    }

    public ParseTree implement(JavaRelImplementor implementor)
    {
        // Generate
        //	   HashSet h = new HashSet();
        //	   <<parent>>
        StatementList stmtList = implementor.getStatementList();
        this.var_h = implementor.newVariable();
        stmtList.add(
            new VariableDeclaration(
                new TypeName("java.util.HashSet"),
                var_h.toString(),
                new AllocationExpression(
                    new TypeName("java.util.HashSet"),
                    null)));
        return implementor.visitJavaChild(this, 0, (JavaRel) child);
    }

    public void implementJavaParent(
        JavaRelImplementor implementor,
        int ordinal)
    {
        assert ordinal == 0;

        // Generate
        //	 if (h.add(<<child variable>>)) {
        //	   <<parent-handler>>
        //	 }
        StatementList stmtList = implementor.getStatementList();
        StatementList ifBody = new StatementList();
        stmtList.add(
            new IfStatement(
                new MethodCall(
                    this.var_h,
                    "add",
                    new ExpressionList(
                        OJUtil.box(
                            OJUtil.typeToOJClass(
                                child.getRowType(),
                                implementor.getTypeFactory()),
                            implementor.translateInput(this, 0)))),
                ifBody));
        implementor.bind(this, child);
        implementor.generateParentBody(this, ifBody);
    }
}


// End JavaDistinctRel.java

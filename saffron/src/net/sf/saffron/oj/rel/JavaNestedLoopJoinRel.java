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

import net.sf.saffron.core.SaffronField;
import net.sf.saffron.core.SaffronPlanner;
import net.sf.saffron.core.SaffronType;
import net.sf.saffron.oj.util.OJUtil;
import net.sf.saffron.opt.CallingConvention;
import net.sf.saffron.opt.OptUtil;
import net.sf.saffron.opt.PlanCost;
import net.sf.saffron.opt.VolcanoCluster;
import net.sf.saffron.rel.JoinRel;
import net.sf.saffron.rel.SaffronRel;
import net.sf.saffron.rex.RexNode;
import net.sf.saffron.rex.RexUtil;
import net.sf.saffron.util.Util;
import openjava.ptree.*;

import java.util.Set;


/**
 * Implements the {@link JoinRel} relational expression using the
 * nested-loop algorithm, with output as Java code.
 */
public class JavaNestedLoopJoinRel extends JoinRel
        implements JavaLoopRel, JavaSelfRel
{
    //~ Constructors ----------------------------------------------------------

    public JavaNestedLoopJoinRel(
        VolcanoCluster cluster,
        SaffronRel left,
        SaffronRel right,
        RexNode condition,
        int joinType,
        Set variablesStopped)
    {
        super(cluster,left,right,condition,joinType,variablesStopped);
    }

    //~ Methods ---------------------------------------------------------------

    public CallingConvention getConvention()
    {
        return CallingConvention.JAVA;
    }

    public Object clone()
    {
        return new JavaNestedLoopJoinRel(
            cluster,
            OptUtil.clone(left),
            OptUtil.clone(right),
            RexUtil.clone(condition),
            joinType,
            variablesStopped);
    }

    public PlanCost computeSelfCost(SaffronPlanner planner)
    {
        double dRows =
            left.getRows() * right.getRows() * RexUtil.getSelectivity(condition);
        double dCpu = left.getRows() * right.getRows();
        double dIo = 0;
        return planner.makeCost(dRows,dCpu,dIo);
    }

    public ParseTree implement(JavaRelImplementor implementor)
    {
        Object o = implementor.visitJavaChild(this, 0, (JavaRel) left);
        assert(o == null);
        return null;
    }

    public void implementJavaParent(JavaRelImplementor implementor,
            int ordinal) {
        StatementList stmtList = implementor.getStatementList();
        switch (ordinal) {
        case 0: // called from left
            // Which variables are set by left and used by right
            String [] variables = OptUtil.getVariablesSetAndUsed(left,right);
            for (int i = 0; i < variables.length; i++) {
                String variable = variables[i]; // e.g. "$cor2"
                Variable variableCorrel = implementor.newVariable();
                SaffronRel rel = getQuery().lookupCorrel(variable);
                SaffronType rowType = rel.getRowType();
                Expression exp = implementor.makeReference(variable,this);
                stmtList.add(
                    new VariableDeclaration(
                        OJUtil.toTypeName(rowType),
                        variableCorrel.toString(),
                        exp));
                implementor.bindCorrel(variable,variableCorrel);
            }
            Object o2 = implementor.visitJavaChild(this, 1, (JavaRel) right);
            assert(o2 == null);
            return;

        case 1: // called from right
            if (!condition.isAlwaysTrue()) {
                StatementList ifBody = new StatementList();
                Expression condition2 = implementor.translate(this,condition);
                stmtList.add(new IfStatement(condition2,ifBody));
                stmtList = ifBody;
            }
            implementor.generateParentBody(this,stmtList);
            return;
        default:
            throw Util.newInternal("implement: ordinal=" + ordinal);
        }
    }

    /**
     * Returns a Java expression which yields the current row of this
     * relational expression.
     */
    public Expression implementSelf(JavaRelImplementor implementor)
    {
        ExpressionList args = new ExpressionList();
        final SaffronField [] fields = rowType.getFields();
        for (int i = 0; i < fields.length; i++) {
            SaffronField field = fields[i];
            args.add(implementor.translate(this,
                    cluster.rexBuilder.makeInputRef(field.getType(), i)));
        }
        return new AllocationExpression(OJUtil.toTypeName(rowType),args);
    }
}


// End JavaNestedLoopJoinRel.java

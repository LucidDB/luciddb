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
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.oj.util.OJUtil;
import org.eigenbase.relopt.CallingConvention;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.rel.JoinRel;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexUtil;
import org.eigenbase.util.Util;
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
        RelOptCluster cluster,
        RelNode left,
        RelNode right,
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
            RelOptUtil.clone(left),
            RelOptUtil.clone(right),
            RexUtil.clone(condition),
            joinType,
            variablesStopped);
    }

    public RelOptCost computeSelfCost(RelOptPlanner planner)
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
            String [] variables = RelOptUtil.getVariablesSetAndUsed(left,right);
            for (int i = 0; i < variables.length; i++) {
                String variable = variables[i]; // e.g. "$cor2"
                Variable variableCorrel = implementor.newVariable();
                RelNode rel = getQuery().lookupCorrel(variable);
                RelDataType rowType = rel.getRowType();
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
        final RelDataTypeField [] fields = rowType.getFields();
        for (int i = 0; i < fields.length; i++) {
            RelDataTypeField field = fields[i];
            args.add(implementor.translate(this,
                    cluster.rexBuilder.makeInputRef(field.getType(), i)));
        }
        return new AllocationExpression(OJUtil.toTypeName(rowType),args);
    }
}


// End JavaNestedLoopJoinRel.java

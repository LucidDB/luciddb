/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by the Free
// Software Foundation; either version 2 of the License, or (at your option)
// any later version approved by The Eigenbase Project.
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

package org.eigenbase.oj.rel;

import openjava.mop.OJClass;
import openjava.ptree.*;

import org.eigenbase.oj.util.OJUtil;
import org.eigenbase.rel.*;
import org.eigenbase.relopt.CallingConvention;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptPlanWriter;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexUtil;
import org.eigenbase.runtime.CalcIterator;
import org.eigenbase.sql.fun.*;
import org.eigenbase.util.Util;

/**
 * <code>IterCalcRel</code> is an iterator implementation of a combination of
 * {@link ProjectRel} above an optional {@link FilterRel}.  It takes an
 * iterator as input, and for each row applies the filter condition if defined.
 * Rows passing the filter expression are transformed via projection and
 * returned.  Note that the same object is always returned (with different
 * values), so parents must not buffer the result.
 *
 * <p>Rules:<ul>
 * <li>{@link org.eigenbase.oj.rel.IterRules.IterCalcRule} creates an
 *     IterCalcRel from a {@link org.eigenbase.rel.CalcRel}</li>
 * </ul>
 */
public class IterCalcRel extends ProjectRelBase implements JavaRel
{
    //~ Instance fields -------------------------------------------------------

    private final RexNode condition;
    private RexNode [] childExps;

    //~ Constructors ----------------------------------------------------------

    public IterCalcRel(
        RelOptCluster cluster,
        RelNode child,
        RexNode [] exps,
        RexNode condition,
        String [] fieldNames,
        int flags)
    {
        super(
            cluster, new RelTraitSet(CallingConvention.ITERATOR), child, exps,
            fieldNames, flags);
        assert (child.getConvention() == CallingConvention.ITERATOR);
        this.condition = condition;
        if (condition == null) {
            childExps = exps;
        } else {
            childExps = new RexNode[exps.length + 1];
            System.arraycopy(exps, 0, childExps, 0, exps.length);
            childExps[exps.length] = condition;
        }
    }

    //~ Methods ---------------------------------------------------------------

    public RexNode [] getChildExps()
    {
        return childExps;
    }

    public RexNode getCondition()
    {
        return condition;
    }

    // TODO jvs 10-May-2004: need a computeSelfCost which takes condition into
    // account; maybe inherit from CalcRelBase?
    public void explain(RelOptPlanWriter pw)
    {
        if (condition == null) {
            super.explain(pw);
            return;
        }
        String [] terms = new String[1 + childExps.length];
        defineTerms(terms);
        terms[exps.length + 1] = "condition";
        pw.explain(this, terms);
    }

    public Object clone()
    {
        IterCalcRel clone = new IterCalcRel(
            getCluster(),
            RelOptUtil.clone(getChild()),
            RexUtil.clone(exps),
            (condition == null) ? null : RexUtil.clone(condition),
            Util.clone(fieldNames),
            getFlags());
        clone.inheritTraitsFrom(this);
        return clone;
    }

    public static Expression implementAbstract(
        JavaRelImplementor implementor,
        JavaRel rel,
        Expression childExp,
        Variable varInputRow,
        final RelDataType inputRowType,
        final RelDataType outputRowType,
        RexNode condition,
        RexNode [] exps)
    {
        RelDataTypeFactory typeFactory = implementor.getTypeFactory();
        OJClass outputRowClass = OJUtil.typeToOJClass(
            outputRowType, typeFactory);
        OJClass inputRowClass = OJUtil.typeToOJClass(
            inputRowType, typeFactory);

        Variable varOutputRow = implementor.newVariable();

        FieldDeclaration rowVarDecl =
            new FieldDeclaration(new ModifierList(ModifierList.PRIVATE),
                TypeName.forOJClass(outputRowClass),
                varOutputRow.toString(),
                new AllocationExpression(
                    outputRowClass,
                    new ExpressionList()));

        StatementList whileBody = new StatementList();

        whileBody.add(
            new VariableDeclaration(
                TypeName.forOJClass(inputRowClass),
                varInputRow.toString(),
                new CastExpression(
                    TypeName.forOJClass(inputRowClass),
                    new MethodCall(
                        new FieldAccess("inputIterator"),
                        "next",
                        new ExpressionList()))));

        MemberDeclarationList memberList = new MemberDeclarationList();

        StatementList condBody;
        if (condition != null) {
            condBody = new StatementList();
            RexNode rexIsTrue =
                rel.getCluster().getRexBuilder().makeCall(
                    SqlStdOperatorTable.isTrueOperator,
                    new RexNode [] { condition });
            Expression conditionExp =
                implementor.translateViaStatements(rel, rexIsTrue, whileBody,
                    memberList);
            whileBody.add(new IfStatement(conditionExp, condBody));
        } else {
            condBody = whileBody;
        }

        RelDataTypeField [] fields = outputRowType.getFields();
        for (int i = 0; i < exps.length; i++) {
            String javaFieldName = Util.toJavaId(
                    fields[i].getName(),
                    i);
            Expression lhs = new FieldAccess(varOutputRow, javaFieldName);
            RexNode rhs = exps[i];
            implementor.translateAssignment(
                rel,
                fields[i].getType(),
                lhs,
                rhs,
                condBody,
                memberList);
        }

        condBody.add(new ReturnStatement(varOutputRow));

        WhileStatement whileStmt =
            new WhileStatement(new MethodCall(
                    new FieldAccess("inputIterator"),
                    "hasNext",
                    new ExpressionList()),
                whileBody);

        StatementList nextMethodBody = new StatementList();
        nextMethodBody.add(whileStmt);
        nextMethodBody.add(new ReturnStatement(Literal.constantNull()));

        MemberDeclaration nextMethodDecl =
            new MethodDeclaration(new ModifierList(ModifierList.PROTECTED),
                OJUtil.typeNameForClass(Object.class), "calcNext",
                new ParameterList(), null, nextMethodBody);

        memberList.add(rowVarDecl);
        memberList.add(nextMethodDecl);
        Expression newIteratorExp =
            new AllocationExpression(
                OJUtil.typeNameForClass(CalcIterator.class),
                new ExpressionList(childExp),
                memberList);

        return newIteratorExp;
    }

    public ParseTree implement(JavaRelImplementor implementor)
    {
        Expression childExp =
            implementor.visitJavaChild(this, 0, (JavaRel) getChild());
        RelDataType outputRowType = getRowType();
        RelDataType inputRowType = getChild().getRowType();

        Variable varInputRow = implementor.newVariable();
        implementor.bind(getChild(), varInputRow);

        return implementAbstract(implementor, this, childExp, varInputRow,
            inputRowType, outputRowType, condition, exps);
    }
}


// End IterCalcRel.java

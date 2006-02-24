/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2006 The Eigenbase Project
// Copyright (C) 2002-2006 Disruptive Tech
// Copyright (C) 2005-2006 LucidEra, Inc.
// Portions Copyright (C) 2003-2006 John V. Sichi
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
import org.eigenbase.oj.rex.RexToOJTranslator;
import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.runtime.CalcIterator;
import org.eigenbase.runtime.CalcTupleIter;
import org.eigenbase.runtime.TupleIter;
import org.eigenbase.sql.fun.*;
import org.eigenbase.util.Util;

import java.util.*;
import java.util.List;

/**
 * <code>IterCalcRel</code> is an iterator implementation of a combination of
 * {@link ProjectRel} above an optional {@link FilterRel}.  It takes a
 * {@link TupleIter iterator} as input, and for each row applies the filter condition if defined.
 * Rows passing the filter expression are transformed via projection and
 * returned.  Note that the same object is always returned (with different
 * values), so parents must not buffer the result.
 *
 * <p>Rules:<ul>
 * <li>{@link org.eigenbase.oj.rel.IterRules.IterCalcRule} creates an
 *     IterCalcRel from a {@link org.eigenbase.rel.CalcRel}</li>
 * </ul>
 */
public class IterCalcRel extends SingleRel implements JavaRel
{
    //~ Instance fields -------------------------------------------------------

    private final RexProgram program;

    /** Values defined in {@link ProjectRelBase.Flags}. */
    protected int flags;

    //~ Constructors ----------------------------------------------------------

    public IterCalcRel(
        RelOptCluster cluster,
        RelNode child,
        RexProgram program,
        int flags)
    {
        super(cluster, new RelTraitSet(CallingConvention.ITERATOR), child);
        assert child.getConvention() == CallingConvention.ITERATOR;
        this.flags = flags;
        this.program = program;
        this.rowType = program.getOutputRowType();
    }

    //~ Methods ---------------------------------------------------------------

    // TODO jvs 10-May-2004: need a computeSelfCost which takes condition into
    // account; maybe inherit from CalcRelBase?

    public void explain(RelOptPlanWriter pw)
    {
        program.explainCalc(this, pw);
    }

    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        double dRows = getChild().getRows();
        double dCpu = getChild().getRows() * program.getExprCount();
        double dIo = 0;
        return planner.makeCost(dRows, dCpu, dIo);
    }

    public Object clone()
    {
        IterCalcRel clone = new IterCalcRel(
            getCluster(),
            RelOptUtil.clone(getChild()),
            program.copy(),
            getFlags());
        clone.inheritTraitsFrom(this);
        return clone;
    }

    protected RelDataType deriveRowType()
    {
        return super.deriveRowType();    //To change body of overridden methods use File | Settings | File Templates.
    }

    public int getFlags()
    {
        return flags;
    }

    public boolean isBoxed()
    {
        return (flags & ProjectRelBase.Flags.Boxed) == ProjectRelBase.Flags.Boxed;
    }

    /**
     * Burrows into a synthetic record and returns the underlying relation
     * which provides the field called <code>fieldName</code>.
     */
    public JavaRel implementFieldAccess(
        JavaRelImplementor implementor,
        String fieldName)
    {
        if (!isBoxed()) {
            return implementor.implementFieldAccess(
                (JavaRel) getChild(), fieldName);
        }
        RelDataType type = getRowType();
        int field = type.getFieldOrdinal(fieldName);
        RexLocalRef ref = program.getProjectList().get(field);
        final int index = ref.getIndex();
        return implementor.findRel(
            (JavaRel) this, program.getExprList().get(index));
    }

    public static Expression implementAbstract(
        JavaRelImplementor implementor,
        JavaRel rel,
        Expression childExp,
        Variable varInputRow,
        final RelDataType inputRowType,
        final RelDataType outputRowType,
        RexProgram program)
    {
        if (CallingConvention.ENABLE_NEW_ITER) {
            return implementAbstractNewIter(
                implementor, rel, childExp, varInputRow, inputRowType,
                outputRowType, program);
        }
        
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
        RexToOJTranslator translator =
            implementor.newStmtTranslator(rel, whileBody, memberList);
        try {
            translator.pushProgram(program);
            if (program.getCondition() != null) {
                condBody = new StatementList();
                RexNode rexIsTrue =
                    rel.getCluster().getRexBuilder().makeCall(
                        SqlStdOperatorTable.isTrueOperator,
                        new RexNode [] { program.getCondition() });
                Expression conditionExp =
                    translator.translateRexNode(rexIsTrue);
                whileBody.add(new IfStatement(conditionExp, condBody));
            } else {
                condBody = whileBody;
            }

            RexToOJTranslator condTranslator = translator.push(condBody);
            RelDataTypeField [] fields = outputRowType.getFields();
            final List<RexLocalRef> projectRefList = program.getProjectList();
            int i = -1;
            for (RexLocalRef rhs : projectRefList) {
                ++i;
                String javaFieldName = Util.toJavaId(
                    fields[i].getName(),
                    i);
                Expression lhs = new FieldAccess(varOutputRow, javaFieldName);
                condTranslator.translateAssignment(fields[i], lhs, rhs);
            }
        } finally {
            translator.popProgram(program);
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

    private static Expression implementAbstractNewIter(
        JavaRelImplementor implementor,
        JavaRel rel,
        Expression childExp,
        Variable varInputRow,
        final RelDataType inputRowType,
        final RelDataType outputRowType,
        RexProgram program)
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

        Variable varInputObj = implementor.newVariable();

        whileBody.add(
            new VariableDeclaration(
                OJUtil.typeNameForClass(Object.class),
                varInputObj.toString(),
                new MethodCall(
                    new FieldAccess("inputIterator"),
                    "fetchNext",
                    new ExpressionList())));

        StatementList ifNoDataReasonBody = new StatementList();

        whileBody.add(
            new IfStatement(
                new InstanceofExpression(
                    varInputObj,
                    OJUtil.typeNameForClass(TupleIter.NoDataReason.class)),
                ifNoDataReasonBody));

        ifNoDataReasonBody.add(
            new ReturnStatement(varInputObj));

        whileBody.add(
            new VariableDeclaration(
                TypeName.forOJClass(inputRowClass),
                varInputRow.toString(),
                new CastExpression(
                    TypeName.forOJClass(inputRowClass),
                    varInputObj)));

        MemberDeclarationList memberList = new MemberDeclarationList();

        StatementList condBody;
        RexToOJTranslator translator =
            implementor.newStmtTranslator(rel, whileBody, memberList);
        try {
            translator.pushProgram(program);
            if (program.getCondition() != null) {
                condBody = new StatementList();
                RexNode rexIsTrue =
                    rel.getCluster().getRexBuilder().makeCall(
                        SqlStdOperatorTable.isTrueOperator,
                        new RexNode [] { program.getCondition() });
                Expression conditionExp =
                    translator.translateRexNode(rexIsTrue);
                whileBody.add(new IfStatement(conditionExp, condBody));
            } else {
                condBody = whileBody;
            }

            RexToOJTranslator condTranslator = translator.push(condBody);
            RelDataTypeField [] fields = outputRowType.getFields();
            final List<RexLocalRef> projectRefList = program.getProjectList();
            int i = -1;
            for (RexLocalRef rhs : projectRefList) {
                ++i;
                String javaFieldName = Util.toJavaId(
                    fields[i].getName(),
                    i);
                Expression lhs = new FieldAccess(varOutputRow, javaFieldName);
                condTranslator.translateAssignment(fields[i], lhs, rhs);
            }
        } finally {
            translator.popProgram(program);
        }

        condBody.add(new ReturnStatement(varOutputRow));

        WhileStatement whileStmt =
            new WhileStatement(
                Literal.makeLiteral(true),
                whileBody);

        StatementList nextMethodBody = new StatementList();
        nextMethodBody.add(whileStmt);

        MemberDeclaration fetchNextMethodDecl =
            new MethodDeclaration(
                new ModifierList(ModifierList.PUBLIC),
                OJUtil.typeNameForClass(Object.class), "fetchNext",
                new ParameterList(), null, nextMethodBody);

        memberList.add(rowVarDecl);
        memberList.add(fetchNextMethodDecl);
        Expression newTupleIterExp =
            new AllocationExpression(
                OJUtil.typeNameForClass(CalcTupleIter.class),
                new ExpressionList(childExp),
                memberList);

        return newTupleIterExp;
    }

    public ParseTree implement(JavaRelImplementor implementor)
    {
        Expression childExp =
            implementor.visitJavaChild(this, 0, (JavaRel) getChild());
        RelDataType outputRowType = getRowType();
        RelDataType inputRowType = getChild().getRowType();

        Variable varInputRow = implementor.newVariable();
        implementor.bind(getChild(), varInputRow);

        return implementAbstract(
            implementor, this, childExp, varInputRow, inputRowType,
            outputRowType, program);
    }

    public RexProgram getProgram()
    {
        return program;
    }
}


// End IterCalcRel.java

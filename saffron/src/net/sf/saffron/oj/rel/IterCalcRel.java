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

import net.sf.saffron.core.PlanWriter;
import net.sf.saffron.core.SaffronField;
import net.sf.saffron.core.SaffronType;
import net.sf.saffron.oj.util.OJUtil;
import net.sf.saffron.opt.CallingConvention;
import net.sf.saffron.opt.OptUtil;
import net.sf.saffron.opt.VolcanoCluster;
import net.sf.saffron.rel.*;
import net.sf.saffron.rex.RexNode;
import net.sf.saffron.rex.RexUtil;
import net.sf.saffron.runtime.CalcIterator;
import net.sf.saffron.sql.SqlOperatorTable;
import net.sf.saffron.util.Util;
import openjava.mop.OJClass;
import openjava.ptree.*;

/**
 * <code>IterCalcRel</code> is an iterator implementation of a combination of
 * {@link ProjectRel} above an optional {@link FilterRel}.  It takes an
 * iterator as input, and for each row applies the filter condition if defined.
 * Rows passing the filter expression are transformed via projection and
 * returned.  Note that the same object is always returned (with different
 * values), so parents must not buffer the result.
 *
 * <p>Rules:<ul>
 * <li>{@link net.sf.saffron.oj.OJPlannerFactory.IterCalcRule} creates an
 *     IterCalcRel from a {@link net.sf.saffron.rel.CalcRel}</li>
 * </ul>
 */
public class IterCalcRel extends ProjectRelBase implements JavaRel
{
    public final RexNode condition;

    private RexNode [] childExps;

    //~ Constructors ----------------------------------------------------------

    public IterCalcRel(
        VolcanoCluster cluster,
        SaffronRel child,
        RexNode [] exps,
        RexNode condition,
        String [] fieldNames,
        int flags)
    {
        super(cluster,child,exps,fieldNames,flags);
        assert(child.getConvention() == CallingConvention.ITERATOR);
        this.condition = condition;
        if (condition == null) {
            childExps = exps;
        } else {
            childExps = new RexNode[exps.length + 1];
            System.arraycopy(exps,0,childExps,0,exps.length);
            childExps[exps.length] = condition;
        }
    }

    //~ Methods ---------------------------------------------------------------

    public RexNode [] getChildExps()
    {
        return childExps;
    }

    // TODO jvs 10-May-2004: need a computeSelfCost which takes condition into
    // account; maybe inherit from a new CalcRelBase?

    public void explain(PlanWriter pw)
    {
        if (condition == null) {
            super.explain(pw);
            return;
        }
        String [] terms = new String[1 + childExps.length];
        defineTerms(terms);
        terms[exps.length + 1] = "condition";
        pw.explain(this,terms);
    }

    public CallingConvention getConvention()
    {
        return CallingConvention.ITERATOR;
    }

    public Object clone()
    {
        return new IterCalcRel(
            cluster,
            OptUtil.clone(child),
            RexUtil.clone(exps),
            (condition == null) ? null : RexUtil.clone(condition),
            Util.clone(fieldNames),
            getFlags());
    }

    public static Expression implementAbstract(
        JavaRelImplementor implementor,
        JavaRel rel,
        Expression childExp,
        Variable varInputRow,
        final SaffronType inputRowType,
        final SaffronType outputRowType,
        RexNode condition,
        RexNode [] exps)
    {
        OJClass outputRowClass = OJUtil.typeToOJClass(outputRowType);
        OJClass inputRowClass = OJUtil.typeToOJClass(inputRowType);

        Variable varOutputRow = implementor.newVariable();

        FieldDeclaration rowVarDecl = new FieldDeclaration(
            new ModifierList(ModifierList.PRIVATE),
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
            RexNode rexIsTrue = rel.getCluster().rexBuilder.makeCall(
                SqlOperatorTable.std().isTrueOperator,
                new RexNode[]{condition});
            Expression conditionExp = implementor.translateViaStatements(
                rel,rexIsTrue,whileBody,memberList);
            whileBody.add(
                new IfStatement(
                    conditionExp,
                    condBody));
        } else {
            condBody = whileBody;
        }

        // TODO:  if projection is identity, just return the underlying row
        // instead

        SaffronField [] fields = outputRowType.getFields();
        for (int i = 0; i < exps.length; i++) {
            String javaFieldName = Util.toJavaId(fields[i].getName());
            Expression lhs = new FieldAccess(varOutputRow,javaFieldName);
            RexNode rhs = exps[i];
            implementor.translateAssignment(
                rel,
                fields[i].getType(),
                lhs,
                rhs,
                condBody,
                memberList);
        }

        condBody.add(
            new ReturnStatement(varOutputRow));

        WhileStatement whileStmt = new WhileStatement(
            new MethodCall(
                new FieldAccess("inputIterator"),
                "hasNext",
                new ExpressionList()),
            whileBody);

        StatementList nextMethodBody = new StatementList();
        nextMethodBody.add(whileStmt);
        nextMethodBody.add(
            new ReturnStatement(Literal.constantNull()));

        MemberDeclaration nextMethodDecl =
            new MethodDeclaration(
                new ModifierList(ModifierList.PROTECTED),
                TypeName.forClass(Object.class),
                "calcNext",
                new ParameterList(),
                null,
                nextMethodBody);

        memberList.add(rowVarDecl);
        memberList.add(nextMethodDecl);
        Expression newIteratorExp = new AllocationExpression(
            TypeName.forClass(CalcIterator.class),
            new ExpressionList(childExp),
            memberList);

        return newIteratorExp;
    }

    public ParseTree implement(JavaRelImplementor implementor)
    {
        Expression childExp =
                implementor.visitJavaChild(this, 0, (JavaRel) child);
        SaffronType outputRowType = getRowType();
        SaffronType inputRowType = child.getRowType();

        Variable varInputRow = implementor.newVariable();
        implementor.bind(child,varInputRow);

        return implementAbstract(
            implementor,
            this,
            childExp,
            varInputRow,
            inputRowType,
            outputRowType,
            condition,
            exps);
    }
}

// End IterCalcRel.java

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
package org.eigenbase.rex;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.RelOptPlanWriter;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.sql.type.SqlTypeUtil;

import java.util.*;

/**
 * A collection of expressions which read inputs, compute output expressions,
 * and optionally use a condition to filter rows.
 *
 * <p>Programs are immutable. It may help to use a {@link RexProgramBuilder},
 * which has the same relationship to {@link RexProgram} as
 * {@link StringBuffer} does has to {@link String}.
 *
 * <p>A program can contain aggregate functions. If it does, the arguments to
 * each aggregate function must be an {@link RexInputRef}.
 *
 * @see RexProgramBuilder
 * @author jhyde
 * @since Aug 18, 2005
 * @version $Id$
 */
public class RexProgram
{
    /**
     * First stage of expression evaluation. The expressions in this array
     * can refer to inputs (using input ordinal #0) or previous expressions
     * in the array (using input ordinal #1).
     */
    private final RexNode[] exprs;

    /**
     * With {@link #condition}, the second stage of expression evaluation.
     */
    private final RexLocalRef[] projects;

    /**
     * The optional condition. If null, the calculator does not filter rows.
     */
    private final RexLocalRef condition;

    private final RelDataType inputRowType;

    /**
     * Whether this program contains aggregates.
     *
     * TODO: obsolete this
     */
    private boolean aggs;
    private final RelDataType outputRowType;
    private final List<RexLocalRef> projectReadOnlyList;
    private final List<RexNode> exprReadOnlyList;

    /**
     * Creates a program.
     *
     * @param inputRowType Input row type
     * @param exprs Common expressions
     * @param projects Projection expressions
     * @param condition Condition expression. If null, calculator does not
     *    filter rows
     * @param outputRowType Description of the row produced by the program
     *
     * @pre !containCommonExprs(exprs)
     * @pre !containForwardRefs(exprs)
     * @pre !containNonTrivialAggs(exprs)
     */
    public RexProgram(
        RelDataType inputRowType,
        RexNode[] exprs,
        RexLocalRef[] projects,
        RexLocalRef condition,
        RelDataType outputRowType)
    {
        this.inputRowType = inputRowType;
        this.exprs = exprs.clone();
        this.exprReadOnlyList =
            Collections.unmodifiableList(Arrays.asList(exprs));
        this.projects = projects.clone();
        this.projectReadOnlyList =
            Collections.unmodifiableList(Arrays.asList(projects));
        this.condition = condition;
        this.outputRowType = outputRowType;
        assert isValid(true);
    }

    /**
     * Creates a program from lists of expressions.
     *
     * @param inputRowType Input row type
     * @param exprList List of common expressions
     * @param projectRefList List of projection expressions
     * @param condition Condition expression. If null, calculator does not
     *    filter rows
     * @param outputRowType Description of the row produced by the program
     *
     * @pre !containCommonExprs(exprList)
     * @pre !containForwardRefs(exprList)
     * @pre !containNonTrivialAggs(exprList)
     */
    public RexProgram(
        RelDataType inputRowType,
        List<RexNode> exprList,
        List<RexLocalRef> projectRefList,
        RexLocalRef condition,
        RelDataType outputRowType)
    {
        this(
            inputRowType,
            (RexNode[]) exprList.toArray(new RexNode[exprList.size()]),
            (RexLocalRef[]) projectRefList.toArray(new RexLocalRef[projectRefList.size()]),
            condition,
            outputRowType);
    }

    /**
     * Returns the common sub-expressions of this program.
     * Never null, may be empty, and never contain common sub-expressions.
     *
     * @post return != null
     * @post !containCommonExprs(exprs)
     */
    public List<RexNode> getExprList()
    {
        return exprReadOnlyList;
    }

    /**
     * Returns an array of references to the expressions which this program
     * is to project. Never null, may be empty.
     */
    public List<RexLocalRef> getProjectList()
    {
        return projectReadOnlyList;
    }

    /**
     * Returns the field reference of this program's filter condition, or null
     * if there is no condition.
     */
    public RexLocalRef getCondition()
    {
        return condition;
    }

    /**
     * Creates a program which calculates projections and filters rows based
     * upon a condition.
     * Does not attempt to eliminate common sub-expressions.
     *
     * @param projectExprs Project expressions
     * @param conditionExpr Condition on which to filter rows, or null if
     *   rows are not to be filtered
     * @param outputRowType
     * @param rexBuilder
     * @return A program
     */
    public static RexProgram create(
        RelDataType inputRowType,
        RexNode[] projectExprs,
        RexNode conditionExpr,
        RelDataType outputRowType,
        RexBuilder rexBuilder)
    {
        final RexProgramBuilder programBuilder =
            new RexProgramBuilder(inputRowType, rexBuilder);
        final RelDataTypeField[] fields = outputRowType.getFields();
        for (int i = 0; i < projectExprs.length; i++) {
            programBuilder.addProject(projectExprs[i], fields[i].getName());
        }
        if (conditionExpr != null) {
            programBuilder.addCondition(conditionExpr);
        }
        return programBuilder.getProgram();
    }

    /**
     * Helper method for 'explain' functionality.
     * Creates a list of all expressions (common, project, and condition)
     *
     * @deprecated Not used
     */
    public RexNode[] flatten()
    {
        final List list = new ArrayList();
        list.addAll(Arrays.asList(exprs));
        list.addAll(Arrays.asList(projects));
        if (condition != null) {
            list.add(condition);
        }
        return (RexNode []) list.toArray(new RexNode[list.size()]);
    }

    // description of this calc, chiefly intended for debugging
    public String toString()
    {
        // Intended to produce similar output to explainCalc,
        // but without requiring a RelNode or RelOptPlanWriter.
        List termList = new ArrayList();
        List valueList = new ArrayList();
        collectExplainTerms("", termList, valueList);
        final StringBuffer buf = new StringBuffer("(");
        for (int i = 0; i < valueList.size(); i++) {
            if (i > 0) {
                buf.append(", ");
            }
            buf.append(termList.get(i))
                .append("=[")
                .append(valueList.get(i))
                .append("]");
        }
        buf.append(")");
        return buf.toString();
    }

    /**
     * Writes an explanation of the expressions in this program to a plan
     * writer.
     *
     * @param rel Relational expression which owns this program
     * @param pw Plan writer
     */
    public void explainCalc(
        RelNode rel,
        RelOptPlanWriter pw)
    {
        List termList = new ArrayList();
        List valueList = new ArrayList();
        termList.add("child");
        collectExplainTerms("", termList, valueList);
        String[] terms = (String[]) termList.toArray(new String[termList.size()]);
        Object[] values = (Object[]) valueList.toArray(new Object[valueList.size()]);
        // Relational expressions which contain a program should report their
        // children in a different way than getChildExps().
        assert rel.getChildExps().length == 0;
        pw.explain(rel, terms, values);
    }

    /**
     * Collects the expressions in this program into a list of terms
     * and values.
     *
     * @param prefix Prefix for term names, usually the empty string, but
     *   useful if a relational expression contains more than one program
     * @param termList Output list of terms
     * @param valueList Output list of expressions
     */
    public void collectExplainTerms(
        String prefix,
        List termList,
        List valueList)
    {
        final RelDataTypeField [] inFields = inputRowType.getFields();
        final RelDataTypeField [] outFields = outputRowType.getFields();
        assert outFields.length == projects.length :
            "outFields.length=" + outFields.length +
            ", projects.length=" + projects.length;
        termList.add(prefix + "expr#0" +
            ((inFields.length > 1) ? ".." + (inFields.length - 1) : ""));
        valueList.add("{inputs}");
        for (int i = inFields.length; i < exprs.length; i++) {
            termList.add(prefix + "expr#" + i);
            valueList.add(exprs[i]);
        }
        // If a lot of the fields are simply projections of the underlying
        // expression, try to be a bit less verbose.
        int trivialCount = countTrivial(projects);
        switch (trivialCount) {
        case 0:
            break;
        case 1:
            trivialCount = 0;
            break;
        default:
            termList.add(prefix + "proj#0.." + (trivialCount - 1));
            valueList.add("{exprs}");
            break;
        }
        // Print the non-trivial fields with their names as they appear in the
        // output row type.
        for (int i = trivialCount; i < projects.length; i++) {
            termList.add(prefix + outFields[i].getName());
            valueList.add(projects[i]);
        }
        if (condition != null) {
            termList.add(prefix + "$condition");
            valueList.add(condition);
        }
    }

    /**
     * Returns the number of expressions at the front of an array which are
     * simply projections of the same field.
     */
    private static int countTrivial(RexLocalRef[] refs)
    {
        for (int i = 0; i < refs.length; i++) {
            RexLocalRef ref = refs[i];
            if (ref.getIndex() != i) {
                return i;
            }
        }
        return refs.length;
    }

    /**
     * Returns the number of expressions in this program.
     */
    public int getExprCount()
    {
        return exprs.length +
            projects.length +
            (condition == null ? 0 : 1);
    }

    /**
     * Creates a copy of this program.
     */
    public RexProgram copy()
    {
        return new RexProgram(
            inputRowType,
            exprs,
            projects,
            condition == null ? null : (RexLocalRef) condition.clone(),
            outputRowType);
    }

    /**
     * Creates the identity program.
     */
    public static RexProgram createIdentity(RelDataType rowType)
    {
        final RelDataTypeField[] fields = rowType.getFields();
        final RexLocalRef[] projectRefs = new RexLocalRef[fields.length];
        final RexInputRef[] refs = new RexInputRef[fields.length];
        for (int i = 0; i < refs.length; i++) {
            final RelDataType type = fields[i].getType();
            refs[i] = new RexInputRef(i, type);
            projectRefs[i] = new RexLocalRef(i, type);
        }
        return new RexProgram(rowType, refs, projectRefs, null, rowType);
    }

    public RelDataType getInputRowType()
    {
        return inputRowType;
    }

    public boolean containsAggs()
    {
        return aggs || RexOver.containsOver(this);
    }

    public void setAggs(boolean aggs)
    {
        this.aggs = aggs;
    }

    public RelDataType getOutputRowType()
    {
        return outputRowType;
    }

    /**
     * Checks that this program is valid.
     *
     * <p>If <code>fail</code> is true, executes <code>assert false</code>,
     * so will throw an {@link AssertionError} if assertions are enabled.
     * If <code>fail</code> is false, merely returns whether the program is
     * valid.
     *
     * @param fail Whether to fail
     * @return Whether the program is valid
     * @throws AssertionError if program is invalid and <code>fail</code>
     *   is true and assertions are enabled
     */
    public boolean isValid(boolean fail)
    {
        if (inputRowType == null) {
            assert !fail;
            return false;
        }
        if (exprs == null) {
            assert !fail;
            return false;
        }
        if (projects == null) {
            assert !fail;
            return false;
        }
        if (outputRowType == null) {
            assert !fail;
            return false;
        }
        // If the input row type is a struct (contains fields) then the leading
        // expressions must be references to those fields. But we don't require
        // this if the input row type is, say, a java class.
        if (inputRowType.isStruct()) {
            if (!RexUtil.containIdentity(exprs, inputRowType, fail)) {
                assert !fail;
                return false;
            }
            // None of the other fields should be inputRefs.
            for (int i = inputRowType.getFieldCount(); i < exprs.length; i++) {
                RexNode expr = exprs[i];
                if (expr instanceof RexInputRef) {
                    assert !fail;
                    return false;
                }
            }
        }
        if (false && RexUtil.containCommonExprs(exprs, fail)) { // todo: enable
            assert !fail;
            return false;
        }
        if (RexUtil.containForwardRefs(exprs, inputRowType, fail)) {
            assert !fail;
            return false;
        }
        if (RexUtil.containNonTrivialAggs(exprs, fail)) {
            assert !fail;
            return false;
        }
        final Checker checker = new Checker(fail);
        if (condition != null) {
            if (!SqlTypeUtil.inBooleanFamily(condition.getType())) {
                assert !fail : "condition must be boolean";
                return false;
            }
            condition.accept(checker);
            if (checker.failCount > 0) {
                assert !fail;
                return false;
            }
        }
        for (int i = 0; i < projects.length; i++) {
            projects[i].accept(checker);
            if (checker.failCount > 0) {
                assert !fail;
                return false;
            }
        }
        for (int i = 0; i < exprs.length; i++) {
            exprs[i].accept(checker);
            if (checker.failCount > 0) {
                assert !fail;
                return false;
            }
        }
        return true;
    }

    /**
     * Returns whether an expression always evaluates to null.<p/>
     *
     * Like {@link RexUtil#isNull(RexNode)},
     * null literals are null, and
     * casts of null literals are null.
     * But this method also regards references to null expressions as null.
     *
     * @param expr Expression
     * @return Whether expression always evaluates to null
     */
    public boolean isNull(RexNode expr)
    {
        if (RexLiteral.isNullLiteral(expr)) {
            return true;
        }
        if (expr instanceof RexLocalRef) {
            RexLocalRef inputRef = (RexLocalRef) expr;
            return isNull(exprs[inputRef.index]);
        }
        if (expr.getKind() == RexKind.Cast) {
            return isNull(((RexCall) expr).operands[0]);
        }
        return false;
    }

    /**
     * Fully expands a RexLocalRef back into a pure RexNode tree containing no
     * RexLocalRefs (reversing the effect of common subexpression elimination).
     * For example, <code>program.expandLocalRef(program.getCondition())</code>
     * will return the expansion of a program's condition.
     *
     * @param ref a RexLocalRef from this program
     *
     * @return expanded form
     */
    public RexNode expandLocalRef(RexLocalRef ref)
    {
        // TODO jvs 19-Apr-2006:  assert that ref is part of
        // this program
        ExpansionShuttle shuttle = new ExpansionShuttle();
        return ref.accept(shuttle);
    }

    /**
     * Given a list of collations which hold for the input to this program,
     * returns a list of collations which hold for its output. The result is
     * mutable.
     */
    public List<RelCollation> getCollations(List<RelCollation> inputCollations)
    {
        List<RelCollation> outputCollations = new ArrayList<RelCollation>(1);
        deduceCollations(
            outputCollations, inputRowType.getFieldCount(),
            projectReadOnlyList, inputCollations);
        return outputCollations;
    }

    /**
     * Given a list of expressions and a description of which are ordered,
     * computes a list of collations. The result is mutable.
     */
    public static void deduceCollations(
        List<RelCollation> outputCollations,
        final int sourceCount,
        List<RexLocalRef> refs,
        List<RelCollation> inputCollations)
    {
        int[] targets = new int[sourceCount];
        Arrays.fill(targets, -1);
        for (int i = 0; i < refs.size(); i++) {
            final RexLocalRef ref = refs.get(i);
            final int source = ref.getIndex();
            if (source < sourceCount && targets[source] == -1) {
                targets[source] = i;
            }
        }
        loop:
        for (RelCollation collation : inputCollations) {

            final ArrayList<RelFieldCollation> fieldCollations =
                new ArrayList<RelFieldCollation>(0);
            for (RelFieldCollation fieldCollation :
                collation.getFieldCollations())
            {
                final int source = fieldCollation.getFieldIndex();
                final int target = targets[source];
                if (target < 0) {
                    continue loop;
                }
                fieldCollations.add(
                    new RelFieldCollation(
                        target, fieldCollation.getDirection()));
            }
            // Success -- all of the source fields of this key are mapped
            // to the output.
            outputCollations.add(
                new RelCollationImpl(fieldCollations));
        }
    }

    /**
     * Returns whether the fields on the leading edge of the project list
     * are the input fields.
     * @param fail
     */
    public boolean projectsIdentity(final boolean fail)
    {
        final int fieldCount = inputRowType.getFieldCount();
        if (projects.length < fieldCount) {
            assert !fail :
                "program '" + toString() +
                "' does not project identity for input row type '" +
                inputRowType + "'";
            return false;
        }
        for (int i = 0; i < fieldCount; i++) {
            RexLocalRef project = projects[i];
            if (project.index != i) {
                assert !fail :
                    "program " + toString() +
                    "' does not project identity for input row type '" +
                    inputRowType + "', field #" + i;
                return false;
            }
        }
        return true;
    }

    /**
     * Applies a visitor to an array of expressions and, if specified, a
     * single expression.
     *
     * @param visitor Visitor
     * @param exprs Array of expressions
     * @param expr Single expression, may be null
     */
    public static void apply(
        RexVisitor visitor, RexNode[] exprs, RexNode expr)
    {
        for (int i = 0; i < exprs.length; i++) {
            exprs[i].accept(visitor);
        }
        if (expr != null) {
            expr.accept(visitor);
        }
    }

    /**
     * Visitor which walks over a program and checks validity.
     */
    class Checker extends RexVisitorImpl<Boolean>
    {
        private final boolean fail;
        int failCount = 0;

        public Checker(boolean fail)
        {
            super(true);
            this.fail = fail;
        }

        public Boolean visitLocalRef(RexLocalRef localRef)
        {
            final int index = localRef.getIndex();
            if (index < 0 || index >= exprs.length) {
                assert !fail;
                ++failCount;
                return false;
            }
            if (!RelOptUtil.eq(
                "type1", localRef.getType(),
                "type2", exprs[index].getType(), fail)) {
                assert !fail;
                ++failCount;
                return false;
            }
            return true;
        }

        public Boolean visitFieldAccess(RexFieldAccess fieldAccess)
        {
            super.visitFieldAccess(fieldAccess);
            final RelDataType refType =
                fieldAccess.getReferenceExpr().getType();
            assert refType.isStruct();
            final RelDataTypeField field = fieldAccess.getField();
            final int index = field.getIndex();
            if (index < 0 || index > refType.getFieldCount()) {
                assert !fail;
                ++failCount;
                return false;
            }
            final RelDataTypeField typeField = refType.getFields()[index];
            if (!RelOptUtil.eq(
                "type1", typeField.getType(),
                "type2", fieldAccess.getType(), fail)) {
                assert !fail;
                ++failCount;
                return false;
            }
            return true;
        }
    }
    
    /**
     * A RexShuttle used in the implementation of {@link
     * RexProgram#expandLocalRef}.
     */
    private class ExpansionShuttle extends RexShuttle
    {
        public RexNode visitLocalRef(RexLocalRef localRef)
        {
            RexNode tree = getExprList().get(localRef.getIndex());
            return tree.accept(this);
        }
    }

}

// End RexProgram.java

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

package org.eigenbase.relopt;


import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.io.*;

import openjava.ptree.Expression;
import openjava.ptree.FieldAccess;
import openjava.ptree.Variable;

import org.eigenbase.rel.*;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.rex.*;
import org.eigenbase.util.Util;
import org.eigenbase.oj.util.*;
import org.eigenbase.sql.SqlNode;
import org.eigenbase.sql.SqlOperator;
import org.eigenbase.sql.SqlOperatorTable;
import org.eigenbase.sql.fun.SqlStdOperatorTable;

/**
 * <code>RelOptUtil</code> defines static utility methods for use in optimizing
 * {@link RelNode}s.
 *
 * @author jhyde
 * @version $Id$
 *
 * @since 26 September, 2003
 */
public abstract class RelOptUtil
{
    //~ Static fields/initializers --------------------------------------------

    private static final Variable var0 = new Variable(makeName(0));
    private static final Variable var1 = new Variable(makeName(1));

    //~ Methods ---------------------------------------------------------------

    /**
     * Returns the ordinal of the input represented by the variable
     * <code>name</code>, or -1 if it does not represent an input.
     */
    public static int getInputOrdinal(String name)
    {
        if (name.startsWith("$input")) {
            if (name.equals("$input0")) {
                return 0;
            } else if (name.equals("$input1")) {
                return 1;
            } else {
                throw Util.newInternal("unknown input variable: " + name);
            }
        } else {
            return -1;
        }
    }

    /**
     * Returns a list of variables set by a relational expression or its
     * descendants.
     */
    public static Set getVariablesSet(RelNode rel)
    {
        VariableSetVisitor visitor = new VariableSetVisitor();
        go(visitor, rel);
        return visitor.variables;
    }

    /**
     * Returns a set of distinct variables set by <code>rel0</code> and used
     * by <code>rel1</code>.
     */
    public static String [] getVariablesSetAndUsed(
        RelNode rel0,
        RelNode rel1)
    {
        Set set = getVariablesSet(rel0);
        if (set.size() == 0) {
            return Util.emptyStringArray;
        }
        Set used = getVariablesUsed(rel1);
        if (used.size() == 0) {
            return Util.emptyStringArray;
        }
        ArrayList result = new ArrayList();
        for (Iterator vars = set.iterator(); vars.hasNext();) {
            String s = (String) vars.next();
            if (used.contains(s) && !result.contains(s)) {
                result.add(s);
            }
        }
        if (result.size() == 0) {
            return Util.emptyStringArray;
        }
        String [] resultArray = new String[result.size()];
        result.toArray(resultArray);
        return resultArray;
    }

    /**
     * Returns a set of variables used by a relational expression or its
     * descendants.
     * The set may contain duplicates.
     * The item type is the same as {@link org.eigenbase.rex.RexVariable#getName}
     */
    public static Set getVariablesUsed(RelNode rel)
    {
        final VariableUsedVisitor vuv = new VariableUsedVisitor();
        go(
            new VisitorRelVisitor(vuv) {
                // implement RelVisitor
                public void visit(
                    RelNode p,
                    int ordinal,
                    RelNode parent)
                {
                    p.collectVariablesUsed(vuv.variables);
                    super.visit(p, ordinal, parent);

                    // Important! Remove stopped variables AFTER we visit children.
                    // (which what super.visit() does)
                    vuv.variables.removeAll(p.getVariablesStopped());
                }
            },
            rel);
        return vuv.variables;
    }

    public static RelNode clone(RelNode rel)
    {
        return (RelNode) ((AbstractRelNode) rel).clone();
    }

    public static RelNode [] clone(RelNode [] rels)
    {
        rels = (RelNode []) rels.clone();
        for (int i = 0; i < rels.length; i++) {
            rels[i] = clone(rels[i]);
        }
        return rels;
    }

    public static RelTraitSet clone(RelTraitSet traits)
    {
        return (RelTraitSet)traits.clone();
    }

    public static RelTraitSet mergeTraits(
        RelTraitSet baseTraits, RelTraitSet additionalTraits)
    {
        RelTraitSet result = clone(baseTraits);
        for(int i = 0; i < additionalTraits.size(); i++) {
            RelTrait additionalTrait = additionalTraits.getTrait(i);

            if (i >= result.size()) {
                result.addTrait(additionalTrait);
            } else  {
                result.setTrait(i, additionalTrait);
            }
        }

        return result;
    }

    /**
     * Sets a {@link RelVisitor} going on a given relational expression, and
     * returns the result.
     */
    public static RelNode go(
        RelVisitor visitor,
        RelNode p)
    {
        RelHolder root = new RelHolder(p);
        try {
            visitor.visit(root, -1, null);
        } catch (Throwable e) {
            throw Util.newInternal(e, "while visiting tree");
        }
        return root.get();
    }

    /**
     * Constructs a reference to the <code>field</code><sup>th</sup> field of
     * the <code>ordinal</code><sup>th</sup> input.
     */
    public static FieldAccess makeFieldAccess(
        int ordinal,
        int field)
    {
        return new FieldAccess(
            new Variable(makeName(ordinal)),
            OJSyntheticClass.makeField(field));
    }

    /**
     * Constructs a reference to the <code>field</code><sup>th</sup> field of
     * an expression.
     */
    public static FieldAccess makeFieldAccess(
        Expression expr,
        int field)
    {
        return new FieldAccess(
            expr,
            OJSyntheticClass.makeField(field));
    }

    /**
     * Constructs the name for the <code>ordinal</code>th input.  For example,
     * <code>makeName(0)</code> returns "$input0".
     */
    public static String makeName(int ordinal)
    {
        // avoid a memory allocation for the common cases
        switch (ordinal) {
        case 0:
            return "$input0";
        case 1:
            return "$input1";
        default:
            return "$input" + ordinal;
        }
    }

    public static Variable makeReference(int ordinal)
    {
        // save ourselves a memory allocation for the common cases
        switch (ordinal) {
        case 0:
            return var0;
        case 1:
            return var1;
        default:
            return new Variable(makeName(ordinal));
        }
    }

    public static String toString(RelNode [] a)
    {
        StringBuffer sb = new StringBuffer();
        sb.append("{");
        for (int i = 0; i < a.length; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(a[i].toString());
        }
        sb.append("}");
        return sb.toString();
    }

    public static String [] getFieldNames(RelDataType type)
    {
        RelDataTypeField [] fields = type.getFields();
        String [] names = new String[fields.length];
        for (int i = 0; i < fields.length; ++i) {
            names[i] = fields[i].getName();
        }
        return names;
    }

    public static RelDataType createTypeFromProjection(
        final RelDataType type,
        final RelDataTypeFactory typeFactory,
        final List columnNameList)
    {
        return typeFactory.createStructType(
            new RelDataTypeFactory.FieldInfo() {
                public int getFieldCount()
                {
                    return columnNameList.size();
                }

                public String getFieldName(int index)
                {
                    return (String) columnNameList.get(index);
                }

                public RelDataType getFieldType(int index)
                {
                    int iField = type.getFieldOrdinal(getFieldName(index));
                    return type.getFields()[iField].getType();
                }
            });
    }

    public static boolean areRowTypesEqual(
        RelDataType rowType1,
        RelDataType rowType2)
    {
        if (rowType1 == rowType2) {
            return true;
        }
        int n = rowType1.getFieldList().size();
        if (rowType2.getFieldList().size() != n) {
            return false;
        }
        RelDataTypeField [] f1 = rowType1.getFields();
        RelDataTypeField [] f2 = rowType2.getFields();
        for (int i = 0; i < n; ++i) {
            if (!f1[i].getType().equals(f2[i].getType())) {
                return false;
            }
        }
        return true;
    }

    /**
     * Creates a plan suitable for use in <code>EXITS</code> or <code>IN</code>
     * statements. See {@link org.eigenbase.sql2rel.SqlToRelConverter#convertExists}
     * <p>
     * @param cluster
     * @param seekRel A query rel, for example the resulting rel from
     * 'select * from emp' or 'values (1,2,3)' or '('Foo', 34)'.
     * @param conditions May be null
     * @param extraExpr Column expression to add. "TRUE" for EXISTS and IN
     * @param extraName Name of expression to add.
     * @return relational expression which outer joins a boolean condition
     *   column
     * @pre extraExpr == null || extraName != null
     */
    public static RelNode createExistsPlan(
        RelOptCluster cluster,
        RelNode seekRel,
        RexNode[] conditions,
        RexLiteral extraExpr,
        String extraName) {

        RelNode ret = seekRel;

        RexNode conditionExp = null;
        for (int i = 0; i < conditions.length; i++) {
            if (i == 0) {
                conditionExp = conditions[i];
            } else {
                conditionExp =
                    cluster.getRexBuilder().makeCall(
                        cluster.getRexBuilder().getOpTab().andOperator,
                        conditionExp,
                        conditions[i]);
            }
        }

        if (null != conditionExp) {
            ret  = new FilterRel(cluster, seekRel, conditionExp);
        }

        if (null != extraExpr) {
            final RelDataType rowType = seekRel.getRowType();
            final RelDataTypeField [] fields = rowType.getFields();
            final RexNode [] expressions = new RexNode[fields.length + 1];
            String [] fieldNames = new String[fields.length + 1];
            final RexNode ref = cluster.getRexBuilder().makeRangeReference(
                rowType, 0);
            for (int j = 0; j < fields.length; j++) {
                expressions[j] = cluster.getRexBuilder().makeFieldAccess(
                    ref, j);
                fieldNames[j] = fields[j].getName();
            }
            expressions[fields.length] = extraExpr;
            fieldNames[fields.length] =
                Util.uniqueFieldName(fieldNames, fields.length, extraName);
            ret =
                new ProjectRel(cluster, ret, expressions, fieldNames,
                    ProjectRelBase.Flags.Boxed);
        }

        return ret;
    }

    /**
     * Creates a ProjectRel which accomplishes a rename.
     *
     * @param outputType a row type descriptor whose field names the generated
     * ProjectRel must match
     *
     * @param rel the rel whose output is to be renamed; rel.getRowType() must
     * be the same as outputType except for field names
     *
     * @return generated ProjectRel
     */
    public static ProjectRel createRenameRel(
        RelDataType outputType,
        RelNode rel)
    {
        RelDataType inputType = rel.getRowType();

        int n = outputType.getFieldList().size();
        RexNode [] renameExps = new RexNode[n];
        String [] renameNames = new String[n];

        RelDataTypeField [] inputFields = inputType.getFields();
        RelDataTypeField [] outputFields = outputType.getFields();

        final RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
        for (int i = 0; i < n; ++i) {
            assert (inputFields[i].getType().equals(outputFields[i].getType()));
            renameNames[i] = outputFields[i].getName();
            renameExps[i] =
                rexBuilder.makeInputRef(
                    inputFields[i].getType(),
                    inputFields[i].getIndex());
        }

        ProjectRel renameRel =
            new ProjectRel(
                rel.getCluster(),
                rel,
                renameExps,
                renameNames,
                ProjectRel.Flags.Boxed);

        return renameRel;
    }

    /**
     * Creates a filter which will remove rows containing NULL values.
     *
     * @param rel the rel to be filtered
     *
     * @param fieldOrdinals array of 0-based field ordinals to filter,
     * or null for all fields
     *
     * @return filtered rel
     */
    public static RelNode createNullFilter(
        RelNode rel,
        Integer [] fieldOrdinals)
    {
        RexNode condition = null;
        RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
        RelDataType rowType = rel.getRowType();
        int n;
        if (fieldOrdinals != null) {
            n = fieldOrdinals.length;
        } else {
            n = rowType.getFieldList().size();
        }
        RelDataTypeField [] fields = rowType.getFields();
        for (int i = 0; i < n; ++i) {
            int iField;
            if (fieldOrdinals != null) {
                iField = fieldOrdinals[i].intValue();
            } else {
                iField = i;
            }
            RelDataType type = fields[iField].getType();
            if (!type.isNullable()) {
                continue;
            }
            RexNode newCondition =
                rexBuilder.makeCall(
                    rexBuilder.getOpTab().isNotNullOperator,
                    rexBuilder.makeInputRef(type, iField));
            if (condition == null) {
                condition = newCondition;
            } else {
                condition =
                    rexBuilder.makeCall(rexBuilder.getOpTab().andOperator,
                        condition, newCondition);
            }
        }
        if (condition == null) {
            // no filtering required
            return rel;
        }

        return new FilterRel(
            rel.getCluster(),
            rel,
            condition);
    }

    /**
     * Creates a projection which casts a rel's output to a desired row type.
     *
     * @param rel producer of rows to be converted
     *
     * @param castRowType row type after cast
     *
     * @return conversion rel
     */
    public static RelNode createCastRel(
        RelNode rel,
        RelDataType castRowType)
    {
        RelDataType rowType = rel.getRowType();
        if (areRowTypesEqual(rowType, castRowType)) {
            // nothing to do
            return rel;
        }
        String [] fieldNames = RelOptUtil.getFieldNames(rowType);
        RexNode [] castExps =
            RexUtil.generateCastExpressions(rel.getCluster().getRexBuilder(),
                castRowType, rowType);
        return new ProjectRel(
            rel.getCluster(),
            rel,
            castExps,
            fieldNames,
            ProjectRel.Flags.Boxed);
    }

    /**
     * Creates an AggregateRel which removes all duplicates from the result of
     * an underlying rel.
     *
     * @param rel underlying rel
     *
     * @return rel implementing DISTINCT
     */
    public static RelNode createDistinctRel(
        RelNode rel)
    {
        return new AggregateRel(
            rel.getCluster(),
            rel,
            rel.getRowType().getFieldList().size(),
            new AggregateRel.Call[0]);
    }

    public static boolean analyzeSimpleEquiJoin(
        JoinRel joinRel,
        int [] joinFieldOrdinals)
    {
        RexNode joinExp = joinRel.getCondition();
        if (joinExp.getKind() != RexKind.Equals) {
            return false;
        }
        RexCall binaryExpression = (RexCall) joinExp;
        RexNode leftComparand = binaryExpression.operands[0];
        RexNode rightComparand = binaryExpression.operands[1];
        if (!(leftComparand instanceof RexInputRef)) {
            return false;
        }
        if (!(rightComparand instanceof RexInputRef)) {
            return false;
        }

        final int leftFieldCount =
            joinRel.getLeft().getRowType().getFieldList().size();
        RexInputRef leftFieldAccess = (RexInputRef) leftComparand;
        if (!(leftFieldAccess.getIndex() < leftFieldCount)) {
            // left field must access left side of join
            return false;
        }

        RexInputRef rightFieldAccess = (RexInputRef) rightComparand;
        if (!(rightFieldAccess.getIndex() >= leftFieldCount)) {
            // right field must access right side of join
            return false;
        }

        joinFieldOrdinals[0] = leftFieldAccess.getIndex();
        joinFieldOrdinals[1] = rightFieldAccess.getIndex() - leftFieldCount;
        return true;
    }

    public static void registerAbstractRels(RelOptPlanner planner)
    {
        AggregateRel.register(planner);
        FilterRel.register(planner);
        JoinRel.register(planner);
        CorrelatorRel.register(planner);
        OneRowRel.register(planner);
        ProjectRel.register(planner);
        TableAccessRel.register(planner);
        UnionRel.register(planner);
        IntersectRel.register(planner);
        MinusRel.register(planner);
        CalcRel.register(planner);
        CollectRel.register(planner);
        UncollectRel.register(planner);
        planner.addRule(FilterToCalcRule.instance);
        planner.addRule(ProjectToCalcRule.instance);
        planner.addRule(MergeFilterOntoCalcRule.instance);
        planner.addRule(MergeProjectOntoCalcRule.instance);
    }

    /**
     * Dumps a plan as a string.
     *
     * @param header Header to print before the plan. Ignored if the format
     *               is XML.
     * @param rel    Relational expression to explain.
     * @param asXml Whether to format as XML.
     * @return Plan
     */
    public static String dumpPlan(String header, RelNode rel, boolean asXml)
    {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        if (!header.equals("")) {
            pw.println(header);
        }
        RelOptPlanWriter planWriter;
        if (asXml) {
            planWriter = new RelOptXmlPlanWriter(pw);
        } else {
            planWriter = new RelOptPlanWriter(pw);
        }
        planWriter.setIdPrefix(false);
        rel.explain(planWriter);
        pw.flush();
        return sw.toString();
    }

    //~ Inner Classes ---------------------------------------------------------

    private static class RelHolder extends AbstractRelNode
    {
        RelNode p;

        RelHolder(RelNode p)
        {
            super(p.getCluster(), p.getTraits());
            this.p = p;
        }

        public RelNode [] getInputs()
        {
            return new RelNode [] { p };
        }

        public void childrenAccept(RelVisitor visitor)
        {
            visitor.visit(p, 0, this);
        }

        public Object clone()
        {
            throw new UnsupportedOperationException();
        }

        public void replaceInput(
            int ordinalInParent,
            RelNode p)
        {
            assert (ordinalInParent == 0);
            this.p = p;
        }

        RelNode get()
        {
            return p;
        }
    }

    private static class VariableSetVisitor extends RelVisitor
    {
        HashSet variables = new HashSet();

        // implement RelVisitor
        public void visit(
            RelNode p,
            int ordinal,
            RelNode parent)
        {
            super.visit(p, ordinal, parent);
            p.collectVariablesUsed(variables);

            // Important! Remove stopped variables AFTER we visit children
            // (which what super.visit() does)
            variables.removeAll(p.getVariablesStopped());
        }
    }

    private static class VariableUsedVisitor extends RexShuttle
    {
        HashSet variables = new HashSet();

        public RexNode visit(RexCorrelVariable p)
        {
            variables.add(p.getName());
            return p;
        }
    }

    /**
     * Returns a translation of the IS [NOT] DISTINCT FROM sql opertor
     * @param neg if false then a translation of IS NOT DISTINCT FROM is returned
     */
    public static RexNode isDistinctFrom (
        RexBuilder rexBuilder,
        RexNode x,
        RexNode y,
        boolean neg) {

        RexNode ret = null;
        if (x.getType().isStruct()) {
            assert(y.getType().isStruct());
            RelDataTypeField[] xFields = x.getType().getFields();
            RelDataTypeField[] yFields = x.getType().getFields();
            assert(xFields.length == yFields.length);
            for (int i = 0; i < xFields.length; i++) {
                RelDataTypeField xField = xFields[i];
                RelDataTypeField yField = yFields[i];
                RexNode newX = rexBuilder.makeFieldAccess(x, xField.getIndex());
                RexNode newY = rexBuilder.makeFieldAccess(y, yField.getIndex());
                RexNode newCall =
                    isDistinctFromInternal(rexBuilder, newX, newY, neg);
                if (i > 0) {
                    assert(null != ret);
                    ret = rexBuilder.makeCall(
                        SqlStdOperatorTable.andOperator,
                        ret,
                        newCall);
                } else {
                    assert(null == ret);
                    ret = newCall;
                }
            }
        } else {
            ret = isDistinctFromInternal(rexBuilder, x, y, neg);
        }
        return ret;
    }

    private static RexNode isDistinctFromInternal (
        RexBuilder rexBuilder,
        RexNode x,
        RexNode y,
        boolean neg) {

        SqlStdOperatorTable opTab = SqlStdOperatorTable.instance();
        SqlOperator nullOp;
        SqlOperator eqOp;
        if (neg) {
            nullOp = opTab.isNullOperator;
            eqOp   = opTab.equalsOperator;
        } else {
            nullOp = opTab.isNotNullOperator;
            eqOp   = opTab.notEqualsOperator;
        }
        RexNode[] whenThenElse = new RexNode[] {
                // when x is null
                rexBuilder.makeCall(opTab.isNullOperator, x)
                // then return y is [not] null
                ,rexBuilder.makeCall(nullOp, y)
                // when y is null
                ,rexBuilder.makeCall(opTab.isNullOperator, y)
                // then return x is [not] null
                ,rexBuilder.makeCall(nullOp, x)
                // else return x compared to y
                ,rexBuilder.makeCall(eqOp, x, y)};
        return rexBuilder.makeCall(opTab.caseOperator, whenThenElse);
    }
}


// End RelOptUtil.java

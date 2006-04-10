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

package org.eigenbase.relopt;

import java.util.*;
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
import org.eigenbase.sql.*;
import org.eigenbase.sql.type.*;
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
    public static final String NL = System.getProperty("line.separator");

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
        final VisitorRelVisitor visitor = new VisitorRelVisitor(vuv) {
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
        };
        visitor.go(rel);
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
    public static void go(
        RelVisitor visitor,
        RelNode p)
    {
        try {
            visitor.go(p);
        } catch (Throwable e) {
            throw Util.newInternal(e, "while visiting tree");
        }
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

    /**
     * Returns a list of the names of the fields in a given struct type.
     *
     * @param type Struct type
     * @return List of field names
     *
     * @see #getFieldTypeList(RelDataType)
     * @see #getFieldNames(RelDataType)
     */
    public static List<String> getFieldNameList(RelDataType type)
    {
        RelDataTypeField [] fields = type.getFields();
        List<String> nameList = new ArrayList<String>();
        for (int i = 0; i < fields.length; ++i) {
            nameList.add(fields[i].getName());
        }
        return nameList;
    }

    /**
     * Returns an array of the names of the fields in a given struct type.
     *
     * @param type Struct type
     * @return Array of field types
     * @see #getFieldNameList(RelDataType)
     */
    public static String[] getFieldNames(RelDataType type)
    {
        RelDataTypeField[] fields = type.getFields();
        String[] names = new String[fields.length];
        for (int i = 0; i < fields.length; ++i) {
            names[i] = fields[i].getName();
        }
        return names;
    }

    /**
     * Returns a list of the types of the fields in a given struct type.
     *
     * @param type Struct type
     * @return List of field types
     * @see #getFieldNameList(RelDataType)
     */
    public static List<RelDataType> getFieldTypeList(RelDataType type)
    {
        final RelDataTypeField[] fields = type.getFields();
        final List<RelDataType> typeList = new ArrayList<RelDataType>();
        for (int i = 0; i < fields.length; i++) {
            typeList.add(fields[i].getType());
        }
        return typeList;
    }

    /**
     * Collects the names and types of the fields in a given struct type.
     */
    public static void collectFields(
        RelDataType type,
        List<String> fieldNameList,
        List<RelDataType> typeList)
    {
        final RelDataTypeField[] fields = type.getFields();
        for (int i = 0; i < fields.length; i++) {
            final RelDataTypeField field = fields[i];
            typeList.add(field.getType());
            fieldNameList.add(field.getName());
        }
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
        RelDataType rowType2,
        boolean compareNames)
    {
        if (rowType1 == rowType2) {
            return true;
        }
        if (compareNames) {
            // if types are not identity-equal, then either the names or
            // the types must be different
            return false;
        }
        int n = rowType1.getFieldCount();
        if (rowType2.getFieldCount() != n) {
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
     * Verifies that a row type being added to an equivalence class
     * matches the existing type, raising an assertion if this is not
     * the case.
     *
     * @param originalRel canonical rel for equivalence class
     *
     * @param newRel rel being added to equivalence class
     *
     * @param equivalenceClass object representing equivalence class
     */
    public static void verifyTypeEquivalence(
        RelNode originalRel,
        RelNode newRel,
        Object equivalenceClass)
    {
        RelDataType expectedRowType = originalRel.getRowType();
        RelDataType actualRowType = newRel.getRowType();
        
        // Row types must be the same, except for field names.
        if (areRowTypesEqual(expectedRowType, actualRowType, false)) {
            return;
        }
            
        String s =
            "Cannot add expression of different type to set: "
            + Util.lineSeparator + "set type is "
            + expectedRowType.getFullTypeString()
            + Util.lineSeparator + "expression type is "
            + actualRowType.getFullTypeString()
            + Util.lineSeparator + "set is " + equivalenceClass.toString()
            + Util.lineSeparator
            + "expression is " + newRel.toString();
        throw Util.newInternal(s);
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
                        SqlStdOperatorTable.andOperator,
                        conditionExp,
                        conditions[i]);
            }
        }

        if (null != conditionExp) {
            ret  = CalcRel.createFilter(seekRel, conditionExp);
        }

        if (null != extraExpr) {
            final RelDataType rowType = seekRel.getRowType();
            final RelDataTypeField [] fields = rowType.getFields();
            final RexNode [] expressions = new RexNode[fields.length + 1];
            String [] fieldNames = new String[fields.length + 1];
            final RexNode ref = cluster.getRexBuilder().makeRangeReference(
                rowType, 0, false);
            for (int j = 0; j < fields.length; j++) {
                expressions[j] = cluster.getRexBuilder().makeFieldAccess(
                    ref, j);
                fieldNames[j] = fields[j].getName();
            }
            expressions[fields.length] = extraExpr;
            fieldNames[fields.length] =
                Util.uniqueFieldName(fieldNames, fields.length, extraName);
            ret =
                CalcRel.createProject(ret, expressions, fieldNames);
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
     * @return generated relational expression
     */
    public static RelNode createRenameRel(
        RelDataType outputType,
        RelNode rel)
    {
        RelDataType inputType = rel.getRowType();
        RelDataTypeField [] inputFields = inputType.getFields();
        int n = inputFields.length;

        RelDataTypeField [] outputFields = outputType.getFields();
        assert outputFields.length == n;

        RexNode [] renameExps = new RexNode[n];
        String [] renameNames = new String[n];

        final RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
        for (int i = 0; i < n; ++i) {
            assert (inputFields[i].getType().equals(outputFields[i].getType()));
            renameNames[i] = outputFields[i].getName();
            renameExps[i] =
                rexBuilder.makeInputRef(
                    inputFields[i].getType(),
                    inputFields[i].getIndex());
        }

        return CalcRel.createProject(rel, renameExps, renameNames);
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
            n = rowType.getFieldCount();
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
                    SqlStdOperatorTable.isNotNullOperator,
                    rexBuilder.makeInputRef(type, iField));
            if (condition == null) {
                condition = newCondition;
            } else {
                condition =
                    rexBuilder.makeCall(
                        SqlStdOperatorTable.andOperator,
                        condition, newCondition);
            }
        }
        if (condition == null) {
            // no filtering required
            return rel;
        }

        return CalcRel.createFilter(rel,
            condition);
    }

    /**
     * Creates a projection which casts a rel's output to a desired row type.
     *
     * @param rel producer of rows to be converted
     *
     * @param castRowType row type after cast
     *
     * @param rename if true, use field names from castRowType; if false,
     * preserve field names from rel
     *
     * @return conversion rel
     */
    public static RelNode createCastRel(
        final RelNode rel,
        RelDataType castRowType,
        boolean rename)
    {
        RelDataType rowType = rel.getRowType();
        if (areRowTypesEqual(rowType, castRowType, rename)) {
            // nothing to do
            return rel;
        }
        RexNode[] castExps =
            RexUtil.generateCastExpressions(
                rel.getCluster().getRexBuilder(), castRowType, rowType);
        if (rename) {
            // Use names and types from castRowType.
            return CalcRel.createProject(
                rel,
                castExps,
                getFieldNames(castRowType));
        } else {
            // Use names from rowType, types from castRowType.
            return CalcRel.createProject(
                rel,
                castExps,
                getFieldNames(rowType));
        }
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
            rel.getRowType().getFieldCount(),
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
            joinRel.getLeft().getRowType().getFieldCount();
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
    
    /**
     * Splits out the equi-join components of a join condition, and returns
     * what's left.
     * 
     * For example, given the condition
     * 
     * <blockquote><code>L.A = R.X
     * AND L.B = L.C
     * AND (L.D = 5 OR L.E = R.Y)</code></blockquote>
     * 
     * returns<ul>
     * <li>leftKeys = {A}
     * <li>rightKeys = {X}
     * <li>rest = L.B = L.C AND (L.D = 5 OR L.E = R.Y)</li>
     * </ul>
     * 
     * 
     * @param condition Join condition
     * @param leftKeys The ordinals of the fields from the left input which 
     *                 equi-join keys
     * @param rightKeys The ordinals of the fields from the right input which 
     *                 are equi-join keys
     * @return What's left
     */ 
    public static RexNode splitJoinCondition(
        RelNode left,
        RelNode right,
        RexNode condition,
        List<Integer> leftKeys,
        List<Integer> rightKeys)
    {
        List<RexNode> restList = new ArrayList<RexNode>();
        splitJoinCondition(
            left.getRowType().getFieldCount(),
            condition,
            leftKeys,
            rightKeys,
            restList);
        // Convert the remainders into a list.
        switch (restList.size()) {
        case 0:
            return null;
        case 1:
            return restList.get(0);
        default:
            return left.getCluster().getRexBuilder().makeCall(
                SqlStdOperatorTable.andOperator,
                (RexNode[]) restList.toArray(
                    new RexNode[restList.size()]));
        }
    }

    private static void splitJoinCondition(
        final int leftFieldCount,
        RexNode condition,
        List<Integer> leftKeys,
        List<Integer> rightKeys,
        List<RexNode> nonEquiList)
    {
        if (condition instanceof RexCall) {
            RexCall call = (RexCall) condition;
            if (call.getOperator() == SqlStdOperatorTable.andOperator) {
                for (RexNode operand : call.getOperands()) {
                    splitJoinCondition(leftFieldCount, operand, leftKeys,
                        rightKeys, nonEquiList);
                }
                return;
            }
            if (call.getOperator() == SqlStdOperatorTable.equalsOperator) {
                final RexNode[] operands = call.getOperands();
                if (operands[0] instanceof RexInputRef &&
                    operands[1] instanceof RexInputRef) {
                    RexInputRef op0 = (RexInputRef) operands[0];
                    RexInputRef op1 = (RexInputRef) operands[1];
                    if (op0.getIndex() < leftFieldCount &&
                        op1.getIndex() >= leftFieldCount) {
                        // Arguments were of form 'leftField = rightField'
                        leftKeys.add(op0.getIndex());
                        rightKeys.add(op1.getIndex() - leftFieldCount);
                        return;
                    }
                    if (op1.getIndex() < leftFieldCount &&
                        op0.getIndex() >= leftFieldCount) {
                        // Arguments were of form 'rightField = leftField'
                        leftKeys.add(op1.getIndex());
                        rightKeys.add(op0.getIndex() - leftFieldCount);
                        return;
                    }
                }
                // Arguments were not field references, one from each side, so
                // we fail. Fall through.
            }
        }
        // Add this condition to the list of non-equi-join conditions.
        nonEquiList.add(condition);
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

        // REVIEW jvs 9-Apr-2006: Do we still need these two?  Doesn't the
        // combination of MergeCalcRule, FilterToCalcRule, and
        // ProjectToCalcRule have the same effect?
        planner.addRule(MergeFilterOntoCalcRule.instance);
        planner.addRule(MergeProjectOntoCalcRule.instance);
        
        planner.addRule(MergeCalcRule.instance);
    }

    /**
     * Dumps a plan as a string.
     *
     * @param header Header to print before the plan. Ignored if the format
     *               is XML.
     * @param rel    Relational expression to explain.
     * @param asXml Whether to format as XML.
     * @param detailLevel Detail level.
     * @return Plan
     */
    public static String dumpPlan(
        String header, RelNode rel, boolean asXml,
        SqlExplainLevel detailLevel)
    {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        if (!header.equals("")) {
            pw.println(header);
        }
        RelOptPlanWriter planWriter;
        if (asXml) {
            planWriter = new RelOptXmlPlanWriter(pw, detailLevel);
        } else {
            planWriter = new RelOptPlanWriter(pw, detailLevel);
        }
        planWriter.setIdPrefix(false);
        rel.explain(planWriter);
        pw.flush();
        return sw.toString();
    }

    /**
     * Creates the row type descriptor for the result of a DML operation, which
     * is a single column named ROWCOUNT of type BIGINT.
     *
     * @param typeFactory factory to use for creating type descriptor
     *
     * @return created type
     */
    public static RelDataType createDmlRowType(RelDataTypeFactory typeFactory)
    {
        return typeFactory.createStructType(
            new RelDataType[] {typeFactory.createSqlType(SqlTypeName.Bigint)},
            new String[] {"ROWCOUNT"});
    }

    /**
     * Creates a reference to an output field of a relational expression.
     *
     * @param rel Relational expression
     * @param i Field ordinal; if negative, counts from end, so -1 means the
     *   last field
     */ 
    public static RexNode createInputRef(
        RelNode rel,
        int i)
    {
        final RelDataTypeField[] fields = rel.getRowType().getFields();
        if (i < 0) {
            i = fields.length + i;
        }
        return rel.getCluster().getRexBuilder().makeInputRef(
            fields[i].getType(),
            i);
    }    

    /**
     * Returns whether two types are equal using '='.

     * @param desc1
     * @param type1 First type
     * @param desc2
     * @param type2 Second type
     * @param fail Whether to assert if they are not equal
     *
     * @return Whether the types are equal
     */
    public static boolean eq(
        final String desc1, RelDataType type1,
        final String desc2, RelDataType type2,
        boolean fail)
    {
        if (type1 != type2) {
            assert !fail :
                "type mismatch:" + NL +
                desc1 + ":" + NL +
                type1.getFullTypeString() + NL +
                desc2 + ":" + NL +
                type2.getFullTypeString();
            return false;
        }
        return true;
    }

    /**
     * Returns whether two types are equal using
     * {@link #areRowTypesEqual(RelDataType, RelDataType, boolean)}.
     * Both types must not be null.

     * @param desc1 Description of role of first type
     * @param type1 First type
     * @param desc2 Description of role of second type
     * @param type2 Second type
     * @param fail Whether to assert if they are not equal
     *
     * @return Whether the types are equal
     */
    public static boolean equal(
        final String desc1,
        RelDataType type1,
        final String desc2,
        RelDataType type2,
        boolean fail)
    {
        if (!areRowTypesEqual(type1, type2, false)) {
            assert !fail :
                "Type mismatch:" + NL +
                desc1 + ":" + NL +
                type1.getFullTypeString() + NL +
                desc2 + ":" + NL +
                type2.getFullTypeString();
            return false;
        }
        return true;
    }

    /**
     * Returns a translation of the <code>IS DISTINCT FROM</code> (or
     * <code>IS NOT DISTINCT FROM</code>) sql operator.
     *
     * @param neg if false, returns a translation of IS NOT DISTINCT FROM
     */
    public static RexNode isDistinctFrom(
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

    private static RexNode isDistinctFromInternal(
        RexBuilder rexBuilder,
        RexNode x,
        RexNode y,
        boolean neg)
    {
        SqlOperator nullOp;
        SqlOperator eqOp;
        if (neg) {
            nullOp = SqlStdOperatorTable.isNullOperator;
            eqOp   = SqlStdOperatorTable.equalsOperator;
        } else {
            nullOp = SqlStdOperatorTable.isNotNullOperator;
            eqOp   = SqlStdOperatorTable.notEqualsOperator;
        }
        RexNode[] whenThenElse = {
            // when x is null
            rexBuilder.makeCall(SqlStdOperatorTable.isNullOperator, x),
            // then return y is [not] null
            rexBuilder.makeCall(nullOp, y),
            // when y is null
            rexBuilder.makeCall(SqlStdOperatorTable.isNullOperator, y),
            // then return x is [not] null
            rexBuilder.makeCall(nullOp, x),
            // else return x compared to y
            rexBuilder.makeCall(eqOp, x, y)};
        return rexBuilder.makeCall(
            SqlStdOperatorTable.caseOperator, whenThenElse);
    }

    /**
     * Converts a relational expression to a string.
     */
    public static String toString(final RelNode rel)
    {
        final StringWriter sw = new StringWriter();
        final RelOptPlanWriter planWriter =
            new RelOptPlanWriter(new PrintWriter(sw));
        planWriter.setIdPrefix(false);
        rel.explain(planWriter);
        planWriter.flush();
        String string = sw.toString();
        return string;
    }

    /**
     * Renames a relational expression to make its field names the same as
     * another row type. If the row type is already identical, or if the
     * row type is too different (the fields are different in number or type)
     * does nothing.
     *
     * @param rel Relational expression
     * @param desiredRowType Desired row type (including desired field names)
     * @return Renamed relational expression, or the original expression if
     *   there is nothing to do or nothing we <em>can</em> do.
     */
    public static RelNode renameIfNecessary(
        RelNode rel,
        RelDataType desiredRowType)
    {
        final RelDataType rowType = rel.getRowType();
        if (rowType == desiredRowType) {
            // Nothing to do.
            return rel;
        }
        assert !rowType.equals(desiredRowType);

        if (!areRowTypesEqual(rowType, desiredRowType, false)) {
            // The row types are different ignoring names. Nothing we can do.
            return rel;
        }
        rel =
            CalcRel.createRename(
                rel,
                getFieldNames(desiredRowType));
        return rel;
    }

    public static String dumpType(RelDataType type)
    {
        final StringWriter sw = new StringWriter();
        final PrintWriter pw = new PrintWriter(sw);
        final TypeDumper typeDumper = new TypeDumper(pw);
        if (type.isStruct()) {
            typeDumper.acceptFields(type.getFields());
        } else {
            typeDumper.accept(type);
        }
        pw.flush();
        return sw.toString();
    }
    
    /**
     * Decompose a rex predicate into list of RexNodes that are AND'ed together
     *
     * @param rexPredicate predicate to be analyzed
     * @param rexList list of decomposed RexNodes
     */
    public static void decompCF(RexNode rexPredicate, List<RexNode> rexList)
    {
        if (rexPredicate.isA(RexKind.And)) {
            final RexNode[] operands = ((RexCall) rexPredicate).getOperands();
            for (int i = 0; i < operands.length; i++) {
                RexNode operand = operands[i];
                decompCF(operand, rexList);
            }
        } else {
            rexList.add(rexPredicate);            
        }
    }
    
    /**
     * Ands two sets of join filters together, either of which can be null.
     * 
     * @param rexBuilder rexBuilder to create AND expression
     * @param left filter on the left that the right will be AND'd to
     * @param right filter on the right
     * @return AND'd filter
     */
    public static RexNode andJoinFilters(
        RexBuilder rexBuilder, RexNode left, RexNode right)
    {
        // don't bother AND'ing in expressions that always evaluate to
        // true
        if (left != null && !left.isAlwaysTrue()) {
            if (right != null && !right.isAlwaysTrue()) {
                left = rexBuilder.makeCall(
                    SqlStdOperatorTable.andOperator, left, right);
            }
        } else {
            left = right;
        }
        
        // Joins must have some filter
        if (left == null) {
            left = rexBuilder.makeLiteral(true);
        }
        return left;
    }
    
    /**
     * Clones an expression tree and walks through it, adjusting each 
     * RexInputRef index by some amount
     * 
     * @param rexBuilder builder for creating new RexInputRefs
     * @param fields fields where the RexInputRefs originally originated from
     * @param rex the expression
     * @param adjustment the amount to adjust each field by
     * @return modified expression tree
     */
    public static RexNode convertRexInputRefs(
        RexBuilder rexBuilder,
        RexNode rex,
        RelDataTypeField[] fields,
        int[] adjustments)
    {
        RexNode newFilter = RexUtil.clone(rex);
        newFilter = adjustRexInputRefs(
            rexBuilder, newFilter, fields, adjustments);
        return newFilter;
    }
    
    private static RexNode adjustRexInputRefs(
        RexBuilder rexBuilder,
        RexNode rex,
        RelDataTypeField[] fields,
        int[] adjustments)
    {
        if (rex instanceof RexCall) {
            RexNode [] operands = ((RexCall) rex).operands;
            for (int i = 0; i < operands.length; i++) {
                RexNode operand = operands[i];
                operands[i] = adjustRexInputRefs(
                    rexBuilder, operand, fields, adjustments);
            }
            return rex;
        } else if (rex instanceof RexInputRef) {
            RexInputRef var = (RexInputRef) rex;
            int index = var.getIndex();
            if (adjustments[index] != 0) {
                return rexBuilder.makeInputRef(
                    fields[index].getType(), index + adjustments[index]);
            }
        }
        return rex;
    }
    
    /**
     * Adjusts key values in a list by some fixed amount.
     * 
     * @param keys list of key values
     * @param adjustment the amount to adjust the key values by
     * @return modified list
     */
    public static List<Integer> adjustKeys(List<Integer> keys, int adjustment)
    {
        List<Integer> newKeys = new ArrayList<Integer>();
        for (int i = 0; i < keys.size(); i++) {
            newKeys.add(keys.get(i) + adjustment);
        }
        return newKeys;
    }
    
    /**
     * Sets a bit in a bitmap for each RexInputRef in a RelNode
     * 
     * @param bitmap bitmap to be set
     * @param start starting bit to set, corresponding to first field
     * in the RelNode
     * @param end the bit one beyond the last to be set
     */
    public static void setRexInputBitmap(BitSet bitmap, int start, int end)
    {
        for (int i = start; i < end; i++) {
            bitmap.set(i);
        }
    }
    
    /**
     * Sets a bit in a bitmap for every RexInputRef encountered in an
     * expression
     * 
     * @param rex expression from which to look for RexInputRefs
     * @param rexRefs bitmap representing RexInputRefs contained in the rex
     */
    public static void findRexInputRefs(RexNode rex, BitSet rexRefs)
    {
        if (rex instanceof RexCall) {
            RexNode [] operands = ((RexCall) rex).operands;
            for (int i = 0; i < operands.length; i++) {
                findRexInputRefs(operands[i], rexRefs);
            }
        } else if (rex instanceof RexInputRef) {
            RexInputRef var = (RexInputRef) rex;
            rexRefs.set(var.getIndex());
        }
    }
    
    /**
     * Returns true if all bits set in the second parameter are also set
     * in the first
     * 
     * @param x containing bitmap
     * @param y bitmap to be checked
     * @return true if all bits in the second parameter are set in the first
     */
    public static boolean contains(BitSet x, BitSet y)
    {
        BitSet tmp = new BitSet();
        tmp.or(y);
        tmp.andNot(x);
        return tmp.isEmpty();
    }

    //~ Inner Classes ---------------------------------------------------------

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

    public static class VariableUsedVisitor extends RexShuttle
    {
        public final Set variables = new HashSet();

        public RexNode visitCorrelVariable(RexCorrelVariable p)
        {
            variables.add(p.getName());
            return p;
        }
    }

    public static class TypeDumper
    {
        private final String extraIndent = "  ";
        private String indent;
        private final PrintWriter pw;

        TypeDumper(PrintWriter pw)
        {
            this.pw = pw;
            this.indent = "";
        }

        void accept(RelDataType type)
        {
            if (type.isStruct()) {
                final RelDataTypeField[] fields = type.getFields();
                // RECORD (
                //   I INTEGER NOT NULL,
                //   J VARCHAR(240))
                pw.println("RECORD (");
                String prevIndent = indent;
                this.indent = indent + extraIndent;
                acceptFields(fields);
                this.indent = prevIndent;
                pw.print(")");
                if (!type.isNullable()) {
                    pw.print(" NOT NULL");
                }
            } else if (type instanceof MultisetSqlType) {
                // E.g. "INTEGER NOT NULL MULTISET NOT NULL"
                accept(type.getComponentType());
                pw.print(" MULTISET");
                if (!type.isNullable()) {
                    pw.print(" NOT NULL");
                }
            } else {
                // E.g. "INTEGER"
                // E.g. "VARCHAR(240) CHARACTER SET "ISO-8859-1" COLLATE "ISO-8859-1$en_US$primary" NOT NULL"
                pw.print(type.getFullTypeString());
            }
        }

        private void acceptFields(final RelDataTypeField[] fields)
        {
            for (int i = 0; i < fields.length; i++) {
                RelDataTypeField field = fields[i];
                if (i > 0) {
                    pw.println(",");
                }
                pw.print(indent);
                pw.print(field.getName());
                pw.print(" ");
                accept(field.getType());
            }
        }
    }
}


// End RelOptUtil.java

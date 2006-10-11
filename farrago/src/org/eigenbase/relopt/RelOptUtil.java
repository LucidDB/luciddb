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

import java.io.*;

import java.util.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import openjava.ptree.Expression;
import openjava.ptree.FieldAccess;
import openjava.ptree.Variable;

import org.eigenbase.oj.rel.*;
import org.eigenbase.oj.util.*;
import org.eigenbase.rel.*;
import org.eigenbase.rel.rules.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.*;


/**
 * <code>RelOptUtil</code> defines static utility methods for use in optimizing
 * {@link RelNode}s.
 *
 * @author jhyde
 * @version $Id$
 * @since 26 September, 2003
 */
public abstract class RelOptUtil
{

    //~ Static fields/initializers ---------------------------------------------

    private static final Variable var0 = new Variable(makeName(0));
    private static final Variable var1 = new Variable(makeName(1));
    public static final String NL = System.getProperty("line.separator");

    //~ Methods ----------------------------------------------------------------

    /**
     * Returns the ordinal of the input represented by the variable <code>
     * name</code>, or -1 if it does not represent an input.
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
    public static Set<String> getVariablesSet(RelNode rel)
    {
        VariableSetVisitor visitor = new VariableSetVisitor();
        go(visitor, rel);
        return visitor.variables;
    }

    /**
     * Returns a set of distinct variables set by <code>rel0</code> and used by
     * <code>rel1</code>.
     */
    public static String [] getVariablesSetAndUsed(
        RelNode rel0,
        RelNode rel1)
    {
        Set<String> set = getVariablesSet(rel0);
        if (set.size() == 0) {
            return Util.emptyStringArray;
        }
        Set<String> used = getVariablesUsed(rel1);
        if (used.size() == 0) {
            return Util.emptyStringArray;
        }
        List<String> result = new ArrayList<String>();
        for (String s : set) {
            if (used.contains(s) && !result.contains(s)) {
                result.add(s);
            }
        }
        if (result.size() == 0) {
            return Util.emptyStringArray;
        }
        return result.toArray(new String[result.size()]);
    }

    /**
     * Returns a set of variables used by a relational expression or its
     * descendants. The set may contain duplicates. The item type is the same as
     * {@link org.eigenbase.rex.RexVariable#getName}
     */
    public static Set<String> getVariablesUsed(RelNode rel)
    {
        final VariableUsedVisitor vuv = new VariableUsedVisitor();
        final VisitorRelVisitor visitor =
            new VisitorRelVisitor(vuv) {
                // implement RelVisitor
                public void visit(
                    RelNode p,
                    int ordinal,
                    RelNode parent)
                {
                    p.collectVariablesUsed(vuv.variables);
                    super.visit(p, ordinal, parent);

                    // Important! Remove stopped variables AFTER we visit
                    // children. (which what super.visit() does)
                    vuv.variables.removeAll(p.getVariablesStopped());
                }
            };
        visitor.go(rel);
        return vuv.variables;
    }

    public static RelNode [] clone(RelNode [] rels)
    {
        rels = (RelNode []) rels.clone();
        for (int i = 0; i < rels.length; i++) {
            rels[i] = rels[i].clone();
        }
        return rels;
    }

    public static RelTraitSet clone(RelTraitSet traits)
    {
        return (RelTraitSet) traits.clone();
    }

    public static RelTraitSet mergeTraits(
        RelTraitSet baseTraits,
        RelTraitSet additionalTraits)
    {
        RelTraitSet result = clone(baseTraits);
        for (int i = 0; i < additionalTraits.size(); i++) {
            RelTrait additionalTrait = additionalTraits.getTrait(i);

            if (i >= result.size()) {
                result.addTrait(additionalTrait);
            } else {
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
        return
            new FieldAccess(
                new Variable(makeName(ordinal)),
                OJSyntheticClass.makeField(field));
    }

    /**
     * Constructs a reference to the <code>field</code><sup>th</sup> field of an
     * expression.
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
     * Constructs the name for the <code>ordinal</code>th input. For example,
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
        StringBuilder sb = new StringBuilder();
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
     *
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
     *
     * @return Array of field names
     *
     * @see #getFieldNameList(RelDataType)
     */
    public static String [] getFieldNames(RelDataType type)
    {
        RelDataTypeField [] fields = type.getFields();
        String [] names = new String[fields.length];
        for (int i = 0; i < fields.length; ++i) {
            names[i] = fields[i].getName();
        }
        return names;
    }

    /**
     * Returns a list of the types of the fields in a given struct type.
     *
     * @param type Struct type
     *
     * @return List of field types
     *
     * @see #getFieldNameList(RelDataType)
     * @see #getFieldTypes(RelDataType)
     */
    public static List<RelDataType> getFieldTypeList(RelDataType type)
    {
        final RelDataTypeField [] fields = type.getFields();
        final List<RelDataType> typeList = new ArrayList<RelDataType>();
        for (int i = 0; i < fields.length; i++) {
            typeList.add(fields[i].getType());
        }
        return typeList;
    }
    
    /**
     * Returns an array of the types of the fields in a given struct type.
     *
     * @param type Struct type
     *
     * @return Array of field types
     *
     * @see #getFieldTypeList(RelDataType)
     */
    public static RelDataType [] getFieldTypes(RelDataType type)
    {
        RelDataTypeField [] fields = type.getFields();
        RelDataType [] types = new RelDataType[fields.length];
        for (int i = 0; i < fields.length; i++) {
            types[i] = fields[i].getType();
        }
        return types;
    }
    
    /**
     * Collects the names and types of the fields in a given struct type.
     */
    public static void collectFields(
        RelDataType type,
        List<String> fieldNameList,
        List<RelDataType> typeList)
    {
        final RelDataTypeField [] fields = type.getFields();
        for (int i = 0; i < fields.length; i++) {
            final RelDataTypeField field = fields[i];
            typeList.add(field.getType());
            fieldNameList.add(field.getName());
        }
    }

    public static RelDataType createTypeFromProjection(
        final RelDataType type,
        final RelDataTypeFactory typeFactory,
        final List<String> columnNameList)
    {
        return
            typeFactory.createStructType(new RelDataTypeFactory.FieldInfo() {
                    public int getFieldCount()
                    {
                        return columnNameList.size();
                    }

                    public String getFieldName(int index)
                    {
                        return columnNameList.get(index);
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
     * Verifies that a row type being added to an equivalence class matches the
     * existing type, raising an assertion if this is not the case.
     *
     * @param originalRel canonical rel for equivalence class
     * @param newRel rel being added to equivalence class
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
     * Creates a plan suitable for use in <code>EXISTS</code> or <code>IN</code>
     * statements. See {@link
     * org.eigenbase.sql2rel.SqlToRelConverter#convertExists}
     *
     * Note: this implementation of createExistsPlan is only called from
     *     com.disruptivetech.farrago.rel
     *  The last two arguments do not apply to those invocations and can be removed
     *  from the method.
     *  
     * <p>
     *
     * @param cluster
     * @param seekRel A query rel, for example the resulting rel from 'select *
     * from emp' or 'values (1,2,3)' or '('Foo', 34)'.
     * @param conditions May be null
     * @param extraExpr Column expression to add. "TRUE" for EXISTS and IN
     * @param extraName Name of expression to add.
     *
     * @return relational expression which outer joins a boolean condition
     * column
     *
     * @pre extraExpr == null || extraName != null
     */
    public static RelNode createExistsPlan(
        RelOptCluster cluster,
        RelNode seekRel,
        RexNode [] conditions,
        RexLiteral extraExpr,
        String extraName)
    {
        RelNode ret = seekRel;

        if ((conditions != null) && (conditions.length > 0)) {
            RexNode conditionExp =
                RexUtil.andRexNodeList(
                    cluster.getRexBuilder(),
                    Arrays.asList(conditions));

            ret = CalcRel.createFilter(ret, conditionExp);
        }

        if (extraExpr != null) {
            RexBuilder rexBuilder = cluster.getRexBuilder();
            RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();

            assert (extraExpr == rexBuilder.makeLiteral(true));
        	
            // this should only be called for the exists case
            // first stick an Agg on top of the subquery
            // agg does not like no agg functions so just pretend it is
            // doing a min(TRUE)
        	
            RexNode[] exprs = new RexNode[1];
            exprs[0] = extraExpr;
            
            ret = CalcRel.createProject(ret, exprs, null);            
            RelDataType[] argTypes = new RelDataType[1];
            argTypes[0] = typeFactory.createSqlType(SqlTypeName.Boolean);
            
            SqlAggFunction minFunction = 
                new SqlMinMaxAggFunction(
                    argTypes,
                    true,
                        SqlMinMaxAggFunction.MINMAX_COMPARABLE);
            
            RelDataType returnType = minFunction.inferReturnType(typeFactory, argTypes);
            
            int[] pos = new int[1];
            pos[0]=0;
            
            final AggregateRelBase.Call aggCall =
                new AggregateRelBase.Call(
                    minFunction,
                    false,
                    pos,
                    returnType);
            
            ret =
                new AggregateRel(
                    ret.getCluster(),
                    ret,
                    0,
                    new AggregateRel.Call[] {aggCall});            
        }

        return ret;
    }

    public static RelNode createExistsPlan(
        RelOptCluster cluster,
        RelNode seekRel,
        boolean isIn,
        boolean isExists,
        boolean needsOuterJoin)
    {
        RelNode ret = seekRel;

        if (isIn || isExists) {
            RexBuilder rexBuilder = cluster.getRexBuilder();
            RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();

            RelDataType inputFieldType = ret.getRowType();
            
            int outputFieldCount;
            if (isIn) {
                if (needsOuterJoin) {
                    outputFieldCount = inputFieldType.getFieldCount() + 1;
                } else {
                    outputFieldCount = inputFieldType.getFieldCount();
                }
            } else {
                // EXISTS only projects TRUE in the subquery
                outputFieldCount = 1;
            }

            RexNode[] exprs = new RexNode[outputFieldCount];
            
            // for IN/NOT IN , it needs to output the fields
            if (isIn) {
                for (int i = 0; i < inputFieldType.getFieldCount(); i ++) {
                    exprs[i] = rexBuilder.makeInputRef(
                        inputFieldType.getFields()[i].getType(), i);
                }
            }
            
            if (needsOuterJoin) {
                // First insert an Agg on top of the subquery
                // agg does not like no agg functions so just pretend it is
                // doing a min(TRUE)
                RexNode trueExp = rexBuilder.makeLiteral(true);
                exprs[outputFieldCount-1] = trueExp;
            
                ret = CalcRel.createProject(ret, exprs, null);            

                RelDataType[] argTypes = new RelDataType[1];
                argTypes[0] = typeFactory.createSqlType(SqlTypeName.Boolean);
            
                SqlAggFunction minFunction = 
                    new SqlMinMaxAggFunction(
                        argTypes,
                        true,
                        SqlMinMaxAggFunction.MINMAX_COMPARABLE);
            
                RelDataType returnType = minFunction.inferReturnType(typeFactory, argTypes);
            
                int newProjFieldCount = ret.getRowType().getFieldCount();
            
                int[] pos = new int[1];
                pos[0]=newProjFieldCount-1;
            
                final AggregateRelBase.Call aggCall =
                    new AggregateRelBase.Call(
                        minFunction,
                        false,
                        pos,
                        returnType);
  
                ret =
                    new AggregateRel(
                        ret.getCluster(),
                        ret,
                        newProjFieldCount-1,
                        new AggregateRel.Call[] {aggCall});
            } else {
                ret =
                    new AggregateRel(
                        ret.getCluster(),
                        ret,
                        ret.getRowType().getFieldCount(),
                        new AggregateRel.Call[0]);
            }
        }

        return ret;
    }

    /**
     * Creates a ProjectRel which accomplishes a rename.
     *
     * @param outputType a row type descriptor whose field names the generated
     * ProjectRel must match
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
        assert outputFields.length == n : "rename: field count mismatch: in="
            + inputType
            + ", out" + outputType;

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
     * @param fieldOrdinals array of 0-based field ordinals to filter, or null
     * for all fields
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
                        condition,
                        newCondition);
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
     * @param castRowType row type after cast
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
        RexNode [] castExps =
            RexUtil.generateCastExpressions(
                rel.getCluster().getRexBuilder(),
                castRowType,
                rowType);
        if (rename) {
            // Use names and types from castRowType.
            return
                CalcRel.createProject(
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
     * @return rel implementing SingleValueAgg
     */
    public static RelNode createSingleValueAggRel(
        RelOptCluster cluster,
        RelNode rel)
    {
        assert (rel.getRowType().getFieldCount() == 1);
        RelDataType returnType =
            SqlStdOperatorTable.singleValueOperator.inferReturnType(
                cluster.getRexBuilder().getTypeFactory(),
                new RelDataType[] {rel.getRowType().getFields()[0].getType()});
        
        final AggregateRelBase.Call singleValueCall =
            new AggregateRelBase.Call(
                SqlStdOperatorTable.singleValueOperator,
                false,
                new int[] {0},
                returnType);
        
        return
            new AggregateRel(
                rel.getCluster(),
                rel,
                0,
                new AggregateRel.Call[] { singleValueCall });
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
        return
            new AggregateRel(
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
     * what's left. For example, given the condition
     *
     * <blockquote><code>L.A = R.X AND L.B = L.C AND (L.D = 5 OR L.E =
     * R.Y)</code></blockquote>
     *
     * returns
     *
     * <ul>
     * <li>leftKeys = {A}
     * <li>rightKeys = {X}
     * <li>rest = L.B = L.C AND (L.D = 5 OR L.E = R.Y)</li>
     * </ul>
     *
     * @param left left input to join
     * @param right right input to join
     * @param condition join condition
     * @param leftKeys The ordinals of the fields from the left input which
     * equi-join keys
     * @param rightKeys The ordinals of the fields from the right input which
     * are equi-join keys
     *
     * @return What's left
     */
    public static RexNode splitJoinCondition(
        RelNode left,
        RelNode right,
        RexNode condition,
        List<Integer> leftKeys,
        List<Integer> rightKeys)
    {
        List<RexNode> nonEquiList = new ArrayList<RexNode>();

        splitJoinCondition(
            left.getRowType().getFieldCount(),
            condition,
            leftKeys,
            rightKeys,
            nonEquiList);

        List<RexNode> residualList = new ArrayList<RexNode>();
        residualList.addAll(nonEquiList);

        // Convert the remainders into a list that are AND'ed together.
        switch (residualList.size()) {
        case 0:
            return null;
        case 1:
            return residualList.get(0);
        default:
            return
                left.getCluster().getRexBuilder().makeCall(
                    SqlStdOperatorTable.andOperator,
                    residualList);
        }
    }

    /**
     * Splits out the equi-join components of a join condition, and returns
     * what's left. Projection might be required by the caller to provide join
     * keys that are not direct field references.
     *
     * @param joinRel join node
     * @param leftJoinKeys The join keys from the left input which are equi-join
     * keys
     * @param rightJoinKeys The join keys from the right input which are
     * equi-join keys
     *
     * @return What's left
     */
    public static RexNode splitJoinCondition(
        RelNode joinRel,
        List<RexNode> leftJoinKeys,
        List<RexNode> rightJoinKeys)
    {
        List<RexNode> nonEquiList = new ArrayList<RexNode>();

        splitJoinCondition(
            joinRel,
            ((JoinRel) joinRel).getCondition(),
            leftJoinKeys,
            rightJoinKeys,
            nonEquiList);

        // Convert the remainders into a list that are AND'ed together.
        switch (nonEquiList.size()) {
        case 0:
            return null;
        case 1:
            return nonEquiList.get(0);
        default:
            return
                joinRel.getCluster().getRexBuilder().makeCall(
                    SqlStdOperatorTable.andOperator,
                    nonEquiList);
        }
    }

    private static void splitJoinCondition(
        RelNode joinRel,
        RexNode condition,
        List<RexNode> leftJoinKeys,
        List<RexNode> rightJoinKeys,
        List<RexNode> nonEquiList)
    {
        int leftFieldCount =
            ((JoinRel) joinRel).getLeft().getRowType().getFieldCount();
        int rightFieldCount =
            ((JoinRel) joinRel).getRight().getRowType().getFieldCount();
        int totalFieldCount = leftFieldCount + rightFieldCount;

        RelDataTypeField [] rightFields =
            ((JoinRel) joinRel).getRight().getRowType().getFields();

        RexBuilder rexBuilder = joinRel.getCluster().getRexBuilder();
        RelDataTypeFactory typeFactory = joinRel.getCluster().getTypeFactory();

        // adjustment array
        int [] adjustments = new int[totalFieldCount];
        for (int i = 0; i < leftFieldCount; i++) {
            adjustments[i] = 0;
        }
        for (int i = leftFieldCount; i < totalFieldCount; i++) {
            adjustments[i] = -leftFieldCount;
        }

        if (condition instanceof RexCall) {
            RexCall call = (RexCall) condition;
            if (call.getOperator() == SqlStdOperatorTable.andOperator) {
                for (RexNode operand : call.getOperands()) {
                    splitJoinCondition(joinRel,
                        operand,
                        leftJoinKeys,
                        rightJoinKeys,
                        nonEquiList);
                }
                return;
            }

            RexNode leftKey = null;
            RexNode rightKey = null;

            if (call.getOperator() == SqlStdOperatorTable.equalsOperator) {
                final RexNode [] operands = call.getOperands();
                RexNode op0 = operands[0];
                RexNode op1 = operands[1];

                BitSet projRefs0 = new BitSet(totalFieldCount);
                BitSet projRefs1 = new BitSet(totalFieldCount);

                RelOptUtil.InputFinder inputFinder0 =
                    new RelOptUtil.InputFinder(projRefs0);
                RelOptUtil.InputFinder inputFinder1 =
                    new RelOptUtil.InputFinder(projRefs1);

                op0.accept(inputFinder0);
                op1.accept(inputFinder1);

                if ((projRefs0.nextSetBit(leftFieldCount) < 0)
                    && (projRefs1.nextSetBit(0) >= leftFieldCount)) {
                    leftKey = op0;
                    rightKey = op1;
                } else if ((projRefs1.nextSetBit(leftFieldCount) < 0)
                    && (projRefs0.nextSetBit(0) >= leftFieldCount)) {
                    leftKey = op1;
                    rightKey = op0;
                }

                if ((leftKey != null) && (rightKey != null)) {
                    // replace right Key input ref
                    rightKey =
                        rightKey.accept(
                            new RelOptUtil.RexInputConverter(
                                rexBuilder,
                                rightFields,
                                rightFields,
                                adjustments));

                    RelDataType leftKeyType = leftKey.getType();
                    RelDataType rightKeyType = rightKey.getType();

                    if (leftKeyType != rightKeyType) {
                        // perform casting
                        RelDataType targetKeyType =
                            typeFactory.leastRestrictive(
                                new RelDataType[] { leftKeyType, rightKeyType });

                        if (leftKeyType != targetKeyType) {
                            leftKey =
                                rexBuilder.makeCast(targetKeyType, leftKey);
                        }

                        if (rightKeyType != targetKeyType) {
                            rightKey =
                                rexBuilder.makeCast(targetKeyType, rightKey);
                        }
                    }

                    leftJoinKeys.add(leftKey);
                    rightJoinKeys.add(rightKey);
                    return;
                }
            }

            // all other case, try tranforming the condition transform to
            // equality "join" conditions f(LHS) > 0 ===> ( f(LHS) > 0 ) = TRUE,
            // and make the RHS produce TRUE
            BitSet projRefs = new BitSet(totalFieldCount);
            RelOptUtil.InputFinder inputFinder =
                new RelOptUtil.InputFinder(projRefs);

            condition.accept(inputFinder);
            leftKey = null;
            rightKey = null;

            if (projRefs.nextSetBit(leftFieldCount) < 0) {
                leftKey = condition;
                rightKey = rexBuilder.makeLiteral(true);
            } else if (projRefs.nextSetBit(0) >= leftFieldCount) {
                leftKey = rexBuilder.makeLiteral(true);

                // replace right Key input ref
                rightKey =
                    condition.accept(
                        new RelOptUtil.RexInputConverter(
                            rexBuilder,
                            rightFields,
                            rightFields,
                            adjustments));
            }

            if ((leftKey != null) && (rightKey != null)) {
                leftJoinKeys.add(leftKey);
                rightJoinKeys.add(rightKey);
                return;
            }
        }

        // The operator is not of RexCall type
        // So we fail. Fall through.
        // Add this condition to the list of non-equi-join conditions.
        nonEquiList.add(condition);
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
                    splitJoinCondition(leftFieldCount,
                        operand,
                        leftKeys,
                        rightKeys,
                        nonEquiList);
                }
                return;
            }

            if (call.getOperator() == SqlStdOperatorTable.equalsOperator) {
                final RexNode [] operands = call.getOperands();
                if ((operands[0] instanceof RexInputRef)
                    && (operands[1] instanceof RexInputRef)) {
                    RexInputRef op0 = (RexInputRef) operands[0];
                    RexInputRef op1 = (RexInputRef) operands[1];

                    RexInputRef leftField, rightField;
                    if ((op0.getIndex() < leftFieldCount)
                        && (op1.getIndex() >= leftFieldCount)) {
                        // Arguments were of form 'op0 = op1'
                        leftField = op0;
                        rightField = op1;
                    } else if ((op1.getIndex() < leftFieldCount)
                        && (op0.getIndex() >= leftFieldCount)) {
                        // Arguments were of form 'op1 = op0'
                        leftField = op1;
                        rightField = op0;
                    } else {
                        nonEquiList.add(condition);
                        return;
                    }

                    leftKeys.add(leftField.getIndex());
                    rightKeys.add(rightField.getIndex() - leftFieldCount);
                    return;
                }
                // Arguments were not field references, one from each side, so
                // we fail. Fall through.
            }
        }

        // Add this condition to the list of non-equi-join conditions.
        nonEquiList.add(condition);
    }

    /**
     * Adding projection to the inputs of a join to produce the required join
     * keys.
     *
     * @param inputRels inputs to a join
     * @param leftJoinKeys
     * @param rightJoinKeys
     * @param leftKeys on return this contains the join key positions from the
     * new project rel on the LHS.
     * @param rightKeys on return this contains the join key positions from the
     * new project rel on the RHS.
     * @param outputProj on return this contains the positions of the original
     * join output in the (to be formed by caller) LhxJoinRel. Caller needs to
     * be responsible for adding projection on the new join output.
     */
    public static void projectJoinInputs(
        RelNode [] inputRels,
        List<RexNode> leftJoinKeys,
        List<RexNode> rightJoinKeys,
        List<Integer> leftKeys,
        List<Integer> rightKeys,
        List<Integer> outputProj)
    {
        RelNode leftRel = inputRels[0];
        RelNode rightRel = inputRels[1];
        RexBuilder rexBuilder = leftRel.getCluster().getRexBuilder();

        int origLeftInputSize = leftRel.getRowType().getFieldCount();
        int origRightInputSize = rightRel.getRowType().getFieldCount();

        List<RexNode> newLeftFields = new ArrayList<RexNode>();
        List<String> newLeftFieldNames = new ArrayList<String>();

        List<RexNode> newRightFields = new ArrayList<RexNode>();
        List<String> newRightFieldNames = new ArrayList<String>();
        int leftKeyCount = leftJoinKeys.size();
        int rightKeyCount = rightJoinKeys.size();
        int i;

        for (i = 0; i < origLeftInputSize; i++) {
            newLeftFields.add(
                rexBuilder.makeInputRef(
                    leftRel.getRowType().getFields()[i].getType(),
                    i));
            newLeftFieldNames.add(
                leftRel.getRowType().getFields()[i].getName());
            outputProj.add(i);
        }

        int newLeftKeyCount = 0;
        for (i = 0; i < leftKeyCount; i++) {
            RexNode leftKey = leftJoinKeys.get(i);

            if (leftKey instanceof RexInputRef) {
                // already added to the projected left fields
                // only need to remember the index in the join key list
                leftKeys.add(((RexInputRef) leftKey).getIndex());
            } else {
                newLeftFields.add(leftKey);
                newLeftFieldNames.add(leftKey.toString());
                leftKeys.add(origLeftInputSize + newLeftKeyCount);
                newLeftKeyCount++;
            }
        }

        int leftFieldCount = origLeftInputSize + newLeftKeyCount;
        for (i = 0; i < origRightInputSize; i++) {
            newRightFields.add(
                rexBuilder.makeInputRef(
                    rightRel.getRowType().getFields()[i].getType(),
                    i));
            newRightFieldNames.add(
                rightRel.getRowType().getFields()[i].getName());
            outputProj.add(i + leftFieldCount);
        }

        int newRightKeyCount = 0;
        for (i = 0; i < rightKeyCount; i++) {
            RexNode rightKey = rightJoinKeys.get(i);

            if (rightKey instanceof RexInputRef) {
                // already added to the projected left fields
                // only need to remember the index in the join key list
                rightKeys.add(((RexInputRef) rightKey).getIndex());
            } else {
                newRightFields.add(rightKey);
                newRightFieldNames.add(rightKey.toString());
                rightKeys.add(origRightInputSize + newRightKeyCount);
                newRightKeyCount++;
            }
        }

        // added project if need to produce new keys than the origianl input
        // fields
        if (newLeftKeyCount > 0) {
            leftRel =
                CalcRel.createProject(leftRel,
                    newLeftFields,
                    newLeftFieldNames);
        }

        if (newRightKeyCount > 0) {
            rightRel =
                CalcRel.createProject(rightRel,
                    newRightFields,
                    newRightFieldNames);
        }

        inputRels[0] = leftRel;
        inputRels[1] = rightRel;
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
        planner.addRule(PullConstantsThroughAggregatesRule.instance);
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
     * @param header Header to print before the plan. Ignored if the format is
     * XML.
     * @param rel Relational expression to explain.
     * @param asXml Whether to format as XML.
     * @param detailLevel Detail level.
     *
     * @return Plan
     */
    public static String dumpPlan(
        String header,
        RelNode rel,
        boolean asXml,
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
        return
            typeFactory.createStructType(
                new RelDataType[] {
                    typeFactory.createSqlType(SqlTypeName.Bigint)
                },
                new String[] { "ROWCOUNT" });
    }

    /**
     * Creates a reference to an output field of a relational expression.
     *
     * @param rel Relational expression
     * @param i Field ordinal; if negative, counts from end, so -1 means the
     * last field
     */
    public static RexNode createInputRef(
        RelNode rel,
        int i)
    {
        final RelDataTypeField [] fields = rel.getRowType().getFields();
        if (i < 0) {
            i = fields.length + i;
        }
        return
            rel.getCluster().getRexBuilder().makeInputRef(
                fields[i].getType(),
                i);
    }

    /**
     * Returns whether two types are equal using '='.
     *
     * @param desc1
     * @param type1 First type
     * @param desc2
     * @param type2 Second type
     * @param fail Whether to assert if they are not equal
     *
     * @return Whether the types are equal
     */
    public static boolean eq(
        final String desc1,
        RelDataType type1,
        final String desc2,
        RelDataType type2,
        boolean fail)
    {
        if (type1 != type2) {
            assert !fail : "type mismatch:" + NL
                + desc1 + ":" + NL
                + type1.getFullTypeString() + NL
                + desc2 + ":" + NL
                + type2.getFullTypeString();
            return false;
        }
        return true;
    }

    /**
     * Returns whether two types are equal using {@link
     * #areRowTypesEqual(RelDataType, RelDataType, boolean)}. Both types must
     * not be null.
     *
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
            assert !fail : "Type mismatch:" + NL
                + desc1 + ":" + NL
                + type1.getFullTypeString() + NL
                + desc2 + ":" + NL
                + type2.getFullTypeString();
            return false;
        }
        return true;
    }

    /**
     * Returns a translation of the <code>IS DISTINCT FROM</code> (or <code>IS
     * NOT DISTINCT FROM</code>) sql operator.
     *
     * @param neg if false, returns a translation of IS NOT DISTINCT FROM
     */
    public static RexNode isDistinctFrom(
        RexBuilder rexBuilder,
        RexNode x,
        RexNode y,
        boolean neg)
    {
        RexNode ret = null;
        if (x.getType().isStruct()) {
            assert (y.getType().isStruct());
            RelDataTypeField [] xFields = x.getType().getFields();
            RelDataTypeField [] yFields = x.getType().getFields();
            assert (xFields.length == yFields.length);
            for (int i = 0; i < xFields.length; i++) {
                RelDataTypeField xField = xFields[i];
                RelDataTypeField yField = yFields[i];
                RexNode newX = rexBuilder.makeFieldAccess(
                        x,
                        xField.getIndex());
                RexNode newY = rexBuilder.makeFieldAccess(
                        y,
                        yField.getIndex());
                RexNode newCall =
                    isDistinctFromInternal(rexBuilder, newX, newY, neg);
                if (i > 0) {
                    assert (null != ret);
                    ret =
                        rexBuilder.makeCall(
                            SqlStdOperatorTable.andOperator,
                            ret,
                            newCall);
                } else {
                    assert (null == ret);
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
            eqOp = SqlStdOperatorTable.equalsOperator;
        } else {
            nullOp = SqlStdOperatorTable.isNotNullOperator;
            eqOp = SqlStdOperatorTable.notEqualsOperator;
        }
        RexNode [] whenThenElse =
            {  // when x is null
                rexBuilder.makeCall(SqlStdOperatorTable.isNullOperator, x),
                // then return y is [not] null
                rexBuilder.makeCall(nullOp, y),
                // when y is null
                rexBuilder.makeCall(SqlStdOperatorTable.isNullOperator, y),
                // then return x is [not] null
                rexBuilder.makeCall(nullOp, x),
                // else return x compared to y
                rexBuilder.makeCall(eqOp, x, y) };
        return
            rexBuilder.makeCall(
                SqlStdOperatorTable.caseOperator,
                whenThenElse);
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
        return sw.toString();
    }

    /**
     * Renames a relational expression to make its field names the same as
     * another row type. If the row type is already identical, or if the row
     * type is too different (the fields are different in number or type) does
     * nothing.
     *
     * @param rel Relational expression
     * @param desiredRowType Desired row type (including desired field names)
     *
     * @return Renamed relational expression, or the original expression if
     * there is nothing to do or nothing we <em>can</em> do.
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
        rel = CalcRel.createRename(
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
    public static void decomposeConjunction(RexNode rexPredicate, List<RexNode> rexList)
    {
        if (rexPredicate == null) {
            return;
        }
        if (rexPredicate.isA(RexKind.And)) {
            final RexNode [] operands = ((RexCall) rexPredicate).getOperands();
            for (int i = 0; i < operands.length; i++) {
                RexNode operand = operands[i];
                decomposeConjunction(operand, rexList);
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
     *
     * @return AND'd filter
     */
    public static RexNode andJoinFilters(
        RexBuilder rexBuilder,
        RexNode left,
        RexNode right)
    {
        // don't bother AND'ing in expressions that always evaluate to
        // true
        if ((left != null) && !left.isAlwaysTrue()) {
            if ((right != null) && !right.isAlwaysTrue()) {
                left =
                    rexBuilder.makeCall(
                        SqlStdOperatorTable.andOperator,
                        left,
                        right);
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
     * Adjusts key values in a list by some fixed amount.
     *
     * @param keys list of key values
     * @param adjustment the amount to adjust the key values by
     *
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
     * @param start starting bit to set, corresponding to first field in the
     * RelNode
     * @param end the bit one beyond the last to be set
     */
    public static void setRexInputBitmap(BitSet bitmap, int start, int end)
    {
        for (int i = start; i < end; i++) {
            bitmap.set(i);
        }
    }

    /**
     * Returns true if all bits set in the second parameter are also set in the
     * first
     *
     * @param x containing bitmap
     * @param y bitmap to be checked
     *
     * @return true if all bits in the second parameter are set in the first
     */
    public static boolean contains(BitSet x, BitSet y)
    {
        BitSet tmp = new BitSet();
        tmp.or(y);
        tmp.andNot(x);
        return tmp.isEmpty();
    }

    /**
     * Classifies filters according to where they should be processed. They
     * either stay where they are, are pushed to the join (if they originated
     * from above the join), or are pushed to one of the children. Filters that
     * are pushed are added to list passed in as input parameters.
     *
     * @param joinRel join node
     * @param filters filters to be classified
     * @param pushJoin true if filters originated from above the join node and
     * the join is an inner join
     * @param pushLeft true if filters can be pushed to the left
     * @param pushRight true if filters can be pushed to the right
     * @param joinFilters list of filters to push to the join
     * @param leftFilters list of filters to push to the left child
     * @param rightFilters list of filters to push to the right child
     *
     * @return true if at least one filter was pushed
     */
    public static boolean classifyFilters(
        RelNode joinRel,
        List<RexNode> filters,
        boolean pushJoin,
        boolean pushLeft,
        boolean pushRight,
        List<RexNode> joinFilters,
        List<RexNode> leftFilters,
        List<RexNode> rightFilters)
    {
        RexBuilder rexBuilder = joinRel.getCluster().getRexBuilder();
        boolean filterPushed = false;
        RelDataTypeField [] joinFields = joinRel.getRowType().getFields();
        int nTotalFields = joinFields.length;

        int nFieldsLeft = joinRel.getInputs()[0].getRowType().getFieldCount();
        BitSet leftBitmap = new BitSet(nFieldsLeft);
        BitSet rightBitmap = new BitSet(nTotalFields - nFieldsLeft);

        // set the reference bitmaps for the left and right children
        RelOptUtil.setRexInputBitmap(leftBitmap, 0, nFieldsLeft);
        RelOptUtil.setRexInputBitmap(rightBitmap, nFieldsLeft, nTotalFields);

        ListIterator filterIter = filters.listIterator();
        RelDataTypeField [] rightFields =
            joinRel.getInputs()[1].getRowType().getFields();
        while (filterIter.hasNext()) {
            RexNode filter = (RexNode) filterIter.next();

            BitSet filterBitmap = new BitSet(nTotalFields);
            filter.accept(new InputFinder(filterBitmap));

            // REVIEW - are there any expressions that need special handling
            // and therefore cannot be pushed?

            // filters can be pushed to the left child if the left child
            // does not generate NULLs and the only columns referenced in
            // the filter originate from the left child
            if (pushLeft && RelOptUtil.contains(leftBitmap, filterBitmap)) {
                filterPushed = true;

                // ignore filters that always evaluate to true
                if (!filter.isAlwaysTrue()) {
                    leftFilters.add(filter);
                }
                filterIter.remove();

                // filters can be pushed to the right child if the right child
                // does not generate NULLs and the only columns referenced in
                // the filter originate from the right child
            } else if (pushRight
                && RelOptUtil.contains(rightBitmap, filterBitmap)) {
                filterPushed = true;
                if (!filter.isAlwaysTrue()) {
                    // adjust the field references in the filter to reflect
                    // that fields in the right now shift over to the left;
                    // since we never push filters to a NULL generating
                    // child, the types of the source should match the dest
                    // so we don't need to explicitly pass the destination
                    // fields to RexInputConverter
                    int [] adjustments = new int[nTotalFields];
                    for (int i = 0; i < nFieldsLeft; i++) {
                        adjustments[i] = 0;
                    }
                    for (int i = nFieldsLeft; i < nTotalFields; i++) {
                        adjustments[i] = -nFieldsLeft;
                    }
                    rightFilters.add(
                        filter.accept(
                            new RelOptUtil.RexInputConverter(
                                rexBuilder,
                                joinFields,
                                rightFields,
                                adjustments)));
                }
                filterIter.remove();

                // if the filter can't be pushed to either child and the join
                // is an inner join, push them to the join if they originated
                // from above the join
            } else if (pushJoin) {
                filterPushed = true;
                joinFilters.add(filter);
                filterIter.remove();
            }

            // else, leave the filter where it is
        }

        return filterPushed;
    }

    /**
     * Splits a filter into two lists, depending on whether or not the filter
     * only references its child input
     *
     * @param nChildFields number of fields in the child
     * @param predicate filters that will be split
     * @param pushable returns the list of filters that can be pushed to the
     * child input
     * @param notPushable returns the list of filters that cannot be pushed to
     * the child input
     */
    public static void splitFilters(
        int nChildFields,
        RexNode predicate,
        List<RexNode> pushable,
        List<RexNode> notPushable)
    {
        // convert the filter to a list
        List<RexNode> filterList = new ArrayList<RexNode>();
        RelOptUtil.decomposeConjunction(predicate, filterList);

        // for each filter, if the filter only references the child inputs,
        // then it can be pushed
        BitSet childBitmap = new BitSet(nChildFields);
        RelOptUtil.setRexInputBitmap(childBitmap, 0, nChildFields);
        for (RexNode filter : filterList) {
            BitSet filterRefs = new BitSet();
            filter.accept(new RelOptUtil.InputFinder(filterRefs));
            if (RelOptUtil.contains(childBitmap, filterRefs)) {
                pushable.add(filter);
            } else {
                notPushable.add(filter);
            }
        }
    }

    /**
     * Splits a join condition.
     *
     * @param left Left input to the join
     * @param right Right input to the join
     * @param condition Join condition
     *
     * @return Array holding the output. equiNonEqui[0] is the equi-join
     * condition (or TRUE if empty); nonEqui[0] is rest of the condition.
     */
    public static RexNode [] splitJoinCondition(
        RelNode left,
        RelNode right,
        RexNode condition)
    {
        final RexBuilder rexBuilder = left.getCluster().getRexBuilder();
        final List<Integer> leftKeys = new ArrayList<Integer>();
        final List<Integer> rightKeys = new ArrayList<Integer>();
        final RexNode nonEquiCondition =
            splitJoinCondition(
                left,
                right,
                condition,
                leftKeys,
                rightKeys);
        RexNode equiCondition = rexBuilder.makeLiteral(true);
        assert leftKeys.size() == rightKeys.size();
        final int keyCount = leftKeys.size();
        int offset = left.getRowType().getFieldCount();
        for (int i = 0; i < keyCount; i++) {
            int leftKey = leftKeys.get(i);
            int rightKey = rightKeys.get(i);
            RexNode equi =
                rexBuilder.makeCall(
                    SqlStdOperatorTable.equalsOperator,
                    rexBuilder.makeInputRef(
                        left.getRowType().getFields()[leftKey].getType(),
                        leftKey),
                    rexBuilder.makeInputRef(
                        right.getRowType().getFields()[rightKey].getType(),
                        rightKey + offset));
            createInputRef(right, leftKey);
            if (i == 0) {
                equiCondition = equi;
            } else {
                equiCondition =
                    rexBuilder.makeCall(
                        SqlStdOperatorTable.andOperator,
                        equiCondition,
                        equi);
            }
        }
        return new RexNode[] { equiCondition, nonEquiCondition };
    }
    
    /**
     * Determines if a projection and its input reference identical input
     * references.
     * 
     * @param project projection being examined
     * @param checkNames if true, also compare that the names of the project
     * fields and its child fields 
     * 
     * @return if checkNames is false, true is returned if the project and its
     * child reference the same input references, regardless of the names of
     * the project and child fields; if checkNames is true, then true is
     * returned if the input references are the same but the field names are
     * different
     */
    public static boolean checkProjAndChildInputs(
        ProjectRel project,
        boolean checkNames)
    {
        if (!project.isBoxed()) {
            return false;
        }

        int n = project.getProjectExps().length;
        RelDataType inputType = project.getChild().getRowType();
        if (inputType.getFieldList().size() != n) {
            return false;
        }
        RelDataTypeField [] projFields = project.getRowType().getFields();
        RelDataTypeField [] inputFields = inputType.getFields();
        boolean namesDifferent = false;
        for (int i = 0; i < n; ++i) {
            RexNode exp = project.getProjectExps()[i];
            if (!(exp instanceof RexInputRef)) {
                return false;
            }
            RexInputRef fieldAccess = (RexInputRef) exp;
            if (i != fieldAccess.getIndex()) {
                // can't support reorder yet
                return false;
            }
            if (checkNames) {
                String inputFieldName = inputFields[i].getName();
                String projFieldName = projFields[i].getName();
                if (!projFieldName.equals(inputFieldName)) {
                    namesDifferent = true;
                }
            }
        }

        // inputs are the same; return value depends on the checkNames
        // parameter
        return (!checkNames || namesDifferent);
    }
    
    /**
     * Creates projection expressions reflecting the swapping of a join's input.
     * 
     * @param newJoin the RelNode corresponding to the join with its inputs
     * swapped
     * @param origJoin original JoinRel
     * @param origOrder if true, create the projection expressions to reflect
     * the original (pre-swapped) join projection; otherwise, create the
     * projection to reflect the order of the swapped projection
     * 
     * @return array of expression representing the swapped join inputs
     */
    public static RexNode[] createSwappedJoinExprs(
        RelNode newJoin, JoinRel origJoin, boolean origOrder)
    {
        final RelDataTypeField [] newJoinFields =
            newJoin.getRowType().getFields();
        final RexBuilder rexBuilder = newJoin.getCluster().getRexBuilder();
        final RexNode [] exps = new RexNode[newJoinFields.length];
        final int nFields = origOrder ?
            origJoin.getRight().getRowType().getFieldCount() :
            origJoin.getLeft().getRowType().getFieldCount();
        for (int i = 0; i < exps.length; i++) {
            final int source = (i + nFields) % exps.length;
            RelDataTypeField field = origOrder ?
                newJoinFields[source] :
                newJoinFields[i];
            exps[i] = rexBuilder.makeInputRef(field.getType(), source);
        }
        return exps;
    }
    
    /**
     * Creates a new SetOpRel corresponding to an original SetOpRel with a
     * new set of input children
     * 
     * @param setOpRel the original SetOpRel
     * @param newSetOpInputs the input children
     * 
     * @return new SetOpRel
     */
    public static SetOpRel createNewSetOpRel(
        SetOpRel setOpRel,
        RelNode[] newSetOpInputs)
    {
        SetOpRel newSetOpRel = null;
        RelOptCluster cluster = setOpRel.getCluster();
        if (setOpRel instanceof UnionRel) {
            newSetOpRel =
                new UnionRel(
                    cluster,
                    newSetOpInputs,
                    !setOpRel.isDistinct());
        } else if (setOpRel instanceof IterConcatenateRel) {
            newSetOpRel = 
                new IterConcatenateRel(
                    cluster,
                    newSetOpInputs);
        } else if (setOpRel instanceof IntersectRel) {
            newSetOpRel =
                new IntersectRel(
                    cluster,
                    newSetOpInputs,
                    !setOpRel.isDistinct());
        } else if (setOpRel instanceof MinusRel) {
            newSetOpRel =
                new MinusRel(
                    cluster,
                    newSetOpInputs,
                    !setOpRel.isDistinct());
        }
        return newSetOpRel;
    }
    
    //~ Inner Classes ----------------------------------------------------------

    private static class VariableSetVisitor
        extends RelVisitor
    {
        final Set<String> variables = new HashSet<String>();

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

    public static class VariableUsedVisitor
        extends RexShuttle
    {
        public final Set<String> variables = new HashSet<String>();

        public RexNode visitCorrelVariable(RexCorrelVariable p)
        {
            variables.add(p.getName());
            return p;
        }
    }

    public static class InputReferencedVisitor
        extends RexShuttle
    {
        public final SortedSet<Integer> inputPosReferenced = new TreeSet<Integer>();

        public RexNode visitInputRef(RexInputRef inputRef)
        {
            inputPosReferenced.add(inputRef.getIndex());
            return inputRef;
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
                final RelDataTypeField [] fields = type.getFields();

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
                // E.g. "INTEGER" E.g. "VARCHAR(240) CHARACTER SET "ISO-8859-1"
                // COLLATE "ISO-8859-1$en_US$primary" NOT NULL"
                pw.print(type.getFullTypeString());
            }
        }

        private void acceptFields(final RelDataTypeField [] fields)
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

    /**
     * Visitor which builds a bitmap of the inputs used by an expression.
     */
    public static class InputFinder
        extends RexVisitorImpl<Void>
    {
        private final BitSet rexRefSet;

        public InputFinder(BitSet rexRefSet)
        {
            super(true);
            this.rexRefSet = rexRefSet;
        }

        public Void visitInputRef(RexInputRef inputRef)
        {
            rexRefSet.set(inputRef.getIndex());
            return null;
        }

        /**
         * Applies this visitor to an array of expressions and an optional
         * single expression.
         */
        public void apply(RexNode [] exprs, RexNode expr)
        {
            RexProgram.apply(this, exprs, expr);
        }
    }

    /**
     * Walks an expression tree, converting the index of RexInputRefs based on
     * some adjustment factor.
     */
    public static class RexInputConverter
        extends RexShuttle
    {
        protected final RexBuilder rexBuilder;
        private final RelDataTypeField [] srcFields;
        protected final RelDataTypeField [] destFields;
        private final int [] adjustments;

        /**
         * @param rexBuilder builder for creating new RexInputRefs
         * @param srcFields fields where the RexInputRefs originally originated
         * from
         * @param destFields fields that the new RexInputRefs will be
         * referencing; if null, the types of the srcFields are the same as the
         * destFields
         * @param adjustments the amount to adjust each field by
         */
        public RexInputConverter(
            RexBuilder rexBuilder,
            RelDataTypeField [] srcFields,
            RelDataTypeField [] destFields,
            int [] adjustments)
        {
            this.rexBuilder = rexBuilder;
            this.srcFields = srcFields;
            this.destFields = destFields;
            this.adjustments = adjustments;
        }

        public RexInputConverter(
            RexBuilder rexBuilder,
            RelDataTypeField [] srcFields,
            int [] adjustments)
        {
            this.rexBuilder = rexBuilder;
            this.srcFields = srcFields;
            this.destFields = null;
            this.adjustments = adjustments;
        }

        public RexNode visitInputRef(RexInputRef var)
        {
            int srcIndex = var.getIndex();
            int destIndex = srcIndex + adjustments[srcIndex];

            RelDataType type;
            if (destFields == null) {
                type = srcFields[srcIndex].getType();
            } else {
                type = destFields[destIndex].getType();
            }
            if ((adjustments[srcIndex] != 0)
                || (type != srcFields[srcIndex].getType())) {
                return rexBuilder.makeInputRef(type, destIndex);
            } else {
                return var;
            }
        }
    }
}

// End RelOptUtil.java

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

package org.eigenbase.rex;

import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.sql.SqlAggFunction;
import org.eigenbase.sql.SqlOperator;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.sql.type.SqlTypeUtil;
import org.eigenbase.util.Util;

import java.util.HashMap;
import java.util.Map;
import java.util.List;


/**
 * Utility methods concerning row-expressions.
 *
 * @author jhyde
 * @since Nov 23, 2003
 * @version $Id$
 */
public class RexUtil
{
    //~ Static fields/initializers --------------------------------------------

    public static final RexNode [] emptyExpressionArray = new RexNode[0];

    //~ Methods ---------------------------------------------------------------

    public static double getSelectivity(RexNode exp)
    {
        return 0.5;
    }

    /**
     * Returns a copy of a row-expression.
     */
    public static RexNode clone(RexNode exp)
    {
        return (RexNode) exp.clone();
    }

    /**
     * Returns a copy of an array of row-expressions.
     */
    public static RexNode [] clone(RexNode [] exps)
    {
        if (null == exps) {
            return null;
        }
        RexNode [] exps2 = new RexNode[exps.length];
        for (int i = 0; i < exps.length; i++) {
            exps2[i] = clone(exps[i]);
        }
        return exps2;
    }

    /**
     * Returns a copy of a {@link RexInputRef} array.
     */
    public static RexInputRef [] clone(RexInputRef [] exps)
    {
        if (null == exps) {
            return null;
        }
        RexInputRef [] exps2 = new RexInputRef[exps.length];
        for (int i = 0; i < exps.length; i++) {
            exps2[i] = (RexInputRef) clone(exps[i]);
        }
        return exps2;
    }

    /**
     * Returns a copy of a {@link RexLocalRef} array.
     */
    public static RexLocalRef[] clone(RexLocalRef [] exps)
    {
        if (null == exps) {
            return null;
        }
        RexLocalRef[] exps2 = new RexLocalRef[exps.length];
        for (int i = 0; i < exps.length; i++) {
            exps2[i] = (RexLocalRef) clone(exps[i]);
        }
        return exps2;
    }

    /**
     * Generates a cast from one row type to another
     *
     * @param rexBuilder RexBuilder to use for constructing casts
     *
     * @param lhsRowType target row type
     *
     * @param rhsRowType source row type; fields must be 1-to-1 with
     * lhsRowType, in same order
     *
     * @return cast expressions
     */
    public static RexNode [] generateCastExpressions(
        RexBuilder rexBuilder,
        RelDataType lhsRowType,
        RelDataType rhsRowType)
    {
        int n = rhsRowType.getFieldList().size();
        assert (n == lhsRowType.getFieldList().size());
        RexNode [] rhsExps = new RexNode[n];
        for (int i = 0; i < n; ++i) {
            rhsExps[i] =
                rexBuilder.makeInputRef(
                    rhsRowType.getFields()[i].getType(),
                    i);
        }
        return generateCastExpressions(rexBuilder, lhsRowType, rhsExps);
    }

    /**
     * Generates a cast for a row type.
     *
     * @param rexBuilder RexBuilder to use for constructing casts
     *
     * @param lhsRowType target row type
     *
     * @param rhsExps expressions to be cast
     *
     * @return cast expressions
     */
    public static RexNode [] generateCastExpressions(
        RexBuilder rexBuilder,
        RelDataType lhsRowType,
        RexNode [] rhsExps)
    {
        RelDataTypeField [] lhsFields = lhsRowType.getFields();
        final int fieldCount = lhsFields.length;
        RexNode [] castExps = new RexNode[fieldCount];
        assert fieldCount == rhsExps.length;
        for (int i = 0; i < fieldCount; ++i) {
            castExps[i] =
                maybeCast(rexBuilder, lhsFields[i].getType(), rhsExps[i]);
        }
        return castExps;
    }
    
    /**
     * Casts an expression to desired type, or returns the expression unchanged
     * if it is already the correct type.
     *
     * @param rexBuilder Rex builder
     * @param lhsType Desired type
     * @param expr Expression
     * @return Expression cast to desired type
     */
    public static RexNode maybeCast(
        RexBuilder rexBuilder,
        RelDataType lhsType,
        RexNode expr)
    {
        final RelDataType rhsType = expr.getType();
        if (lhsType.equals(rhsType)) {
            return expr;
        } else {
            return rexBuilder.makeCast(lhsType, expr);
        }
    }


    /**
     * Returns whether a node represents the NULL value.
     *
     * <p>Examples:<ul>
     * <li>For {@link org.eigenbase.rex.RexLiteral} Unknown, returns false.
     * <li>For <code>CAST(NULL AS <i>type</i>)</code>, returns true if
     *     <code>allowCast</code> is true, false otherwise.
     * <li>For <code>CAST(CAST(NULL AS <i>type</i>) AS <i>type</i>))</code>,
     *     returns false.
     * </ul>
     */
    public static boolean isNullLiteral(
        RexNode node,
        boolean allowCast)
    {
        if (node instanceof RexLiteral) {
            RexLiteral literal = (RexLiteral) node;
            if (literal.getTypeName() == SqlTypeName.Null) {
                assert (null == literal.getValue());
                return true;
            } else {
                // We don't regard UNKNOWN -- SqlLiteral(null,Boolean) -- as NULL.
                return false;
            }
        }
        if (allowCast) {
            if (node.isA(RexKind.Cast)) {
                RexCall call = (RexCall) node;
                if (isNullLiteral(call.operands[0], false)) {
                    // node is "CAST(NULL as type)"
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Returns whether a node represents the NULL value or a series of nested
     * CAST(NULL as <TYPE>) calls
     * <br>
     * For Example:<br>
     * isNull(CAST(CAST(NULL as INTEGER) AS VARCHAR(1))) returns true
     */
    public static boolean isNull(RexNode node)
    {
        /* Checks to see if the RexNode is null */
        return RexLiteral.isNullLiteral(node)
            || ((node.getKind() == RexKind.Cast)
            && isNull(((RexCall) node).operands[0]));
    }

    /**
     * Returns whether a given node contains a RexCall with a specified
     * operator.
     *
     * @param operator to look for
     * @param node a RexNode tree
     */
    public static RexCall findOperatorCall(
        final SqlOperator operator,
        RexNode node)
    {
        try {
            RexVisitor visitor = new RexVisitorImpl(true) {
                public void visitCall(RexCall call)
                {
                    if (call.getOperator().equals(operator)) {
                        throw new Util.FoundOne(call);
                    }
                    super.visitCall(call);
                }
            };
            node.accept(visitor);
            return null;
        } catch (Util.FoundOne e) {
            Util.swallow(e, null);
            return (RexCall) e.getNode();
        }
    }

    /**
     * Creates an array of {@link RexInputRef} objects, one for each field of a
     * given rowtype.
     */
    public static RexInputRef[] toInputRefs(RelDataType rowType)
    {
        final RelDataTypeField[] fields = rowType.getFields();
        final RexInputRef[] refs = new RexInputRef[fields.length];
        for (int i = 0; i < refs.length; i++) {
            refs[i] = new RexInputRef(i, fields[i].getType());
        }
        return refs;
    }

    /**
     * Creates an array of {@link RexLocalRef} objects, one for each field of a
     * given rowtype.
     */
    public static RexLocalRef[] toLocalRefs(RelDataType rowType)
    {
        final RelDataTypeField[] fields = rowType.getFields();
        final RexLocalRef[] refs = new RexLocalRef[fields.length];
        for (int i = 0; i < refs.length; i++) {
            refs[i] = new RexLocalRef(i, fields[i].getType());
        }
        return refs;
    }

    /**
     * Creates an array of {@link RexInputRef} objects, one for each field of a
     * given rowtype, according to a permutation.
     *
     * @param args Permutation
     * @param rowType Input row type
     * @return Array of input refs
     */
    public static RexInputRef[] toInputRefs(int[] args, RelDataType rowType)
    {
        final RelDataTypeField[] fields = rowType.getFields();
        final RexInputRef[] rexNodes = new RexInputRef[args.length];
        for (int i = 0; i < args.length; i++) {
            int fieldOrdinal = args[i];
            rexNodes[i] =
                new RexInputRef(fieldOrdinal, fields[fieldOrdinal].getType());
        }
        return rexNodes;
    }

    /**
     * Converts an array of {@link RexNode} to an array of {@link Integer}.
     * Every node must be a {@link RexLocalRef}.
     */
    public static Integer[] toOrdinalArray(RexNode[] rexNodes)
    {
        Integer[] orderKeys = new Integer[rexNodes.length];
        for (int i = 0; i < orderKeys.length; i++) {
            RexLocalRef inputRef = (RexLocalRef) rexNodes[i];
            orderKeys[i] = new Integer(inputRef.getIndex());
        }
        return orderKeys;
    }
    
    /**
     * Collects the types of an array of row expressions.
     *
     * @param exprs array of row expressions
     *
     * @return array of types
     */
    public static RelDataType [] collectTypes(RexNode [] exprs)
    {
        RelDataType [] types = new RelDataType[exprs.length];
        for (int i = 0; i < types.length; i++) {
            types[i] = exprs[i].getType();
        }
        return types;
    }

    /**
     * Determines whether a {@link RexCall} requires decimal expansion. 
     * It usually requires expansion if it has decimal operands. 
     * 
     * <p>Exceptions to this rule are:
     * <ul>
     *   <li>It's okay to cast decimals to and from char types
     *   <li>It's okay to cast null literals as decimals
     *   <li>Casts require expansion if their return type is decimal
     *   <li>Reinterpret casts can handle a decimal operand
     * </ul>
     * 
     * @param expr expression possibly in need of expansion
     * @param recurse whether to check nested calls
     * @return whether the expression requires expansion
     */
    public static boolean requiresDecimalExpansion(
        RexNode expr,
        boolean recurse)
    {
        if (!(expr instanceof RexCall)) {
            return false;
        }
        RexCall call = (RexCall) expr;
        if (call.isA(RexKind.Reinterpret)) {
            return (recurse 
                && requiresDecimalExpansion(call.operands, recurse));
        }
        if (call.isA(RexKind.Cast)) {
            if (isNullLiteral(call.operands[0], false)) {
                return false;
            }
            
            RelDataType lhsType = call.getType();
            RelDataType rhsType = call.operands[0].getType();
            if (SqlTypeUtil.inCharFamily(lhsType)
                || SqlTypeUtil.inCharFamily(rhsType)) 
            {
                return (recurse 
                    && requiresDecimalExpansion(call.operands, recurse));
            }
            if (SqlTypeUtil.isDecimal(lhsType)) {
                return true;
            }
        }
        for (int i = 0; i < call.operands.length; i++) {
            if (SqlTypeUtil.isDecimal(call.operands[i].getType())) {
                return true;
            }
        }
        if (recurse) {
            return requiresDecimalExpansion(call.operands, recurse);
        }
        return false;
    }

    /** Determines whether any operand of a set requires decimal expansion */
    public static boolean requiresDecimalExpansion(
        RexNode[] operands,
        boolean recurse)
    {
        for (int i = 0; i < operands.length; i++) {
            if (operands[i] instanceof RexCall) {
                RexCall call = (RexCall) operands[i];
                if (requiresDecimalExpansion(call, recurse)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Returns whether a {@link RexProgram} contains expressions which require
     * decimal expansion.
     */
    public static boolean requiresDecimalExpansion(
        RexProgram program,
        boolean recurse)
    {
        final List<RexNode> exprList = program.getExprList();
        for (RexNode expr : exprList) {
            if (requiresDecimalExpansion(expr, recurse)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Creates an array of {@link RexInputRef} objects referencing fields {0 ..
     * N} and having types {exprs[0].getType() .. exprs[N].getType()}.
     *
     * @param exprs Expressions whose types to mimic
     * @return An array of input refs of the same length and types as exprs.
     */
    public static RexInputRef[] createIdentityArray(RexNode[] exprs)
    {
        final RexInputRef[] refs = new RexInputRef[exprs.length];
        for (int i = 0; i < refs.length; i++) {
            refs[i] = new RexInputRef(i, exprs[i].getType());
        }
        return refs;
    }

    /**
     * Returns whether an array of expressions has any common sub-expressions.
     */
    public static boolean containCommonExprs(RexNode[] exprs, boolean fail)
    {
        final ExpressionNormalizer visitor = new ExpressionNormalizer(false);
        for (int i = 0; i < exprs.length; i++) {
            try {
                exprs[i].accept(visitor);
            } catch (ExpressionNormalizer.SubExprExistsException e) {
                Util.swallow(e, null);
                assert !fail;
            }
        }
        return false;
    }

    /**
     * Returns whether an array of expressions contains a forward reference.
     * That is, if expression #i contains a {@link RexInputRef} referencing
     * field i or greater.
     *
     * @param exprs Array of expressions
     * @param inputRowType
     * @param fail Whether to assert if there is a forward reference
     * @return Whether there is a forward reference
     */
    public static boolean containForwardRefs(
        RexNode[] exprs,
        RelDataType inputRowType,
        boolean fail)
    {
        final ForwardRefFinder visitor = new ForwardRefFinder(inputRowType);
        for (int i = 0; i < exprs.length; i++) {
            RexNode expr = exprs[i];
            visitor.setLimit(i); // field cannot refer to self or later field
            try {
                expr.accept(visitor);
            } catch (ForwardRefFinder.IllegalForwardRefException e) {
                Util.swallow(e, null);
                assert !fail : "illegal forward reference in " + expr;
                return true;
            }
        }
        return false;
    }

    /**
     * Returns whether an array of exp contains aggregate function calls
     * whose arguments are not {@link RexInputRef}.s
     *
     * @param exprs Expressions
     * @param fail Whether to assert if there is such a function call
     */
    static boolean containNonTrivialAggs(RexNode[] exprs, boolean fail)
    {
        for (int i = 0; i < exprs.length; i++) {
            RexNode expr = exprs[i];
            if (expr instanceof RexCall) {
                RexCall rexCall = (RexCall) expr;
                if (rexCall.getOperator() instanceof SqlAggFunction) {
                    final RexNode[] operands = rexCall.getOperands();
                    for (int j = 0; j < operands.length; j++) {
                        RexNode operand = operands[j];
                        if (!(operand instanceof RexLocalRef)) {
                            assert !fail : "contains non trivial agg";
                            return true;
                        }
                    }
                }
            }
        }
        return false;
    }
    
    /**
     * Returns whether a list of expressions contains complex expressions,
     * that is, a call whose arguments are not {@link RexVariable} (or a
     * subtype such as {@link RexInputRef}) or {@link RexLiteral}.
     */
    public static boolean containComplexExprs(List<RexNode> exprs)
    {
        for (RexNode expr : exprs) {
            if (expr instanceof RexCall) {
                RexCall rexCall = (RexCall) expr;
                final RexNode[] operands = rexCall.getOperands();
                for (int j = 0; j < operands.length; j++) {
                    RexNode operand = operands[j];
                    if (!isAtomic(operand)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    public static boolean isAtomic(RexNode expr)
    {
        return expr instanceof RexLiteral ||
            expr instanceof RexVariable;
    }

    /**
     * Creates a record type with anonymous field names.
     */
    public static RelDataType createStructType(
        RelDataTypeFactory typeFactory,
        final RexNode[] exprs)
    {
        return typeFactory.createStructType(
            new RelDataTypeFactory.FieldInfo()
            {
                public int getFieldCount()
                {
                    return exprs.length;
                }

                public String getFieldName(int index)
                {
                    return "$" + index;
                }

                public RelDataType getFieldType(int index)
                {
                    return exprs[index].getType();
                }
            }
        );
    }

    /**
     * Creates a record type with specified field names.
     *
     * <p>The array of field names may be null, but it is not recommended.
     * If the array is present, its elements must not be null.
     */
    public static RelDataType createStructType(
        RelDataTypeFactory typeFactory,
        final RexNode[] exprs,
        final String[] names)
    {
        return typeFactory.createStructType(
            new RelDataTypeFactory.FieldInfo()
            {
                public int getFieldCount()
                {
                    return exprs.length;
                }

                public String getFieldName(int index)
                {
                    if (names == null) {
                        return "$f" + index;
                    }
                    final String name = names[index];
                    assert name != null;
                    return name;
                }

                public RelDataType getFieldType(int index)
                {
                    return exprs[index].getType();
                }
            }
        );
    }

    /**
     * Returns whether the type of an array of expressions is compatible
     * with a struct type.
     *
     * @param exprs Array of expressions
     * @param type Type
     * @param fail Whether to fail if there is a mismatch
     * @return Whether every expression has the same type as the corresponding
     *   member of the struct type
     *
     * @see RelOptUtil#eq(RelDataType,RelDataType,boolean)
     */
    public static boolean compatibleTypes(
        RexNode[] exprs,
        RelDataType type,
        boolean fail)
    {
        final RelDataTypeField[] fields = type.getFields();
        if (exprs.length != fields.length) {
            assert !fail : "rowtype mismatches expressions";
            return false;
        }
        for (int i = 0; i < fields.length; i++) {
            final RelDataType exprType = exprs[i].getType();
            final RelDataType fieldType = fields[i].getType();
            if (!RelOptUtil.eq(exprType, fieldType, fail)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns whether the leading edge of a given array of expressions is
     * wholly {@link RexInputRef} objects with types corresponding to the
     * underlying datatype.
     */
    public static boolean containIdentity(
        RexNode[] exprs, RelDataType rowType, boolean fail)
    {
        final RelDataTypeField[] fields = rowType.getFields();
        if (exprs.length < fields.length) {
            assert !fail : "exprs/rowType length mismatch";
            return false;
        }
        for (int i = 0; i < fields.length; i++) {
            if (!(exprs[i] instanceof RexInputRef)) {
                assert !fail : "expr[" + i + "] is not a RexInputRef";
                return false;
            }
            RexInputRef inputRef = (RexInputRef) exprs[i];
            if (inputRef.getIndex() != i) {
                assert !fail :
                    "expr[" + i + "] has ordinal " + inputRef.getIndex();
                return false;
            }
            if (!RelOptUtil.eq(exprs[i].getType(), fields[i].getType(), fail)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Walks over expressions and builds a bank of common sub-expressions.
     */
    private static class ExpressionNormalizer extends RexVisitorImpl
    {
        final Map mapDigestToExpr = new HashMap();
        final boolean allowDups;

        protected ExpressionNormalizer(boolean allowDups)
        {
            super(true);
            this.allowDups = allowDups;
        }

        protected void register(RexNode expr)
        {
            final Object previous = mapDigestToExpr.put(expr.toString(), expr);
            if (!allowDups && previous != null) {
                throw new SubExprExistsException(expr);
            }
        }

        protected RexNode lookup(RexNode expr)
        {
            return (RexNode) mapDigestToExpr.get(expr.toString());
        }

        public void visitInputRef(RexInputRef inputRef)
        {
            register(inputRef);
        }

        public void visitLiteral(RexLiteral literal)
        {
            register(literal);
        }

        public void visitCorrelVariable(RexCorrelVariable correlVariable)
        {
            register(correlVariable);
        }

        public void visitCall(RexCall call)
        {
            final RexNode[] operands = call.getOperands();
            RexNode[] normalizedOperands = new RexNode[operands.length];
            int diffCount = 0;
            for (int i = 0; i < operands.length; i++) {
                RexNode operand = operands[i];
                operand.accept(this);
                final RexNode normalizedOperand =
                    normalizedOperands[i] = lookup(operand);
                if (normalizedOperand != operand) {
                    ++diffCount;
                }
            }
            if (diffCount > 0) {
                call = call.clone(call.getType(), normalizedOperands);
            }
            register(call);
        }

        public void visitDynamicParam(RexDynamicParam dynamicParam)
        {
            register(dynamicParam);
        }

        public void visitRangeRef(RexRangeRef rangeRef)
        {
            register(rangeRef);
        }

        public void visitFieldAccess(RexFieldAccess fieldAccess)
        {
            final RexNode expr = fieldAccess.getReferenceExpr();
            expr.accept(this);
            final RexNode normalizedExpr = lookup(expr);
            if (normalizedExpr != expr) {
                fieldAccess = new RexFieldAccess(
                    normalizedExpr, fieldAccess.getField());
            }
            register(fieldAccess);
        }

        /**
         * Thrown if there is a sub-expression.
         */
        private static class SubExprExistsException extends RuntimeException
        {
            SubExprExistsException(RexNode expr) {
                Util.discard(expr);
            }
        }
    }

    /**
     * Walks over an expression and throws an exception if it finds
     * an {@link RexInputRef} with an ordinal beyond the number of fields in
     * the input row type,
     * or a {@link RexLocalRef} with ordinal greater than that set using
     * {@link #setLimit(int)}.
     */
    private static class ForwardRefFinder extends RexVisitorImpl
    {
        private int limit = -1;
        private final RelDataType inputRowType;

        public ForwardRefFinder(RelDataType inputRowType)
        {
            super(true);
            this.inputRowType = inputRowType;
        }

        public void visitInputRef(RexInputRef inputRef)
        {
            super.visitInputRef(inputRef);
            if (inputRef.getIndex() >= inputRowType.getFields().length) {
                throw new IllegalForwardRefException();
            }
        }

        public void visitLocalRef(RexLocalRef inputRef)
        {
            super.visitLocalRef(inputRef);
            if (inputRef.getIndex() >= limit) {
                throw new IllegalForwardRefException();
            }
        }

        public void setLimit(int limit)
        {
            this.limit = limit;
        }

        static class IllegalForwardRefException extends RuntimeException
        {
        }
    }
}


// End RexUtil.java

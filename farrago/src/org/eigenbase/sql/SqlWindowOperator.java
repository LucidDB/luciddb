/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2006 The Eigenbase Project
// Copyright (C) 2004-2006 Disruptive Tech
// Copyright (C) 2005-2006 LucidEra, Inc.
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
package org.eigenbase.sql;

import java.util.*;
import java.math.BigDecimal;

import org.eigenbase.reltype.*;
import org.eigenbase.resource.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.util.*;
import org.eigenbase.sql.validate.*;
import org.eigenbase.util.*;


/**
 * An operator describing a window specification.
 *
 * <p>Operands are as follows:
 *
 * <ul>
 * <li>0: name of referenced window ({@link SqlIdentifier})</li>
 * <li>1: partition clause ({@link SqlNodeList})</li>
 * <li>2: order clause ({@link SqlNodeList})</li>
 * <li>3: isRows ({@link SqlLiteral})</li>
 * <li>4: lowerBound ({@link SqlNode})</li>
 * <li>5: upperBound ({@link SqlNode})</li>
 * </ul>
 *
 * All operands are optional.</p>
 *
 * @author jhyde
 * @version $Id$
 * @since Oct 19, 2004
 */
public class SqlWindowOperator
    extends SqlOperator
{

    //~ Instance fields --------------------------------------------------------

    /**
     * The FOLLOWING operator used exclusively in a window specification.
     */
    static final SqlPostfixOperator followingOperator =
        new SqlPostfixOperator(
            "FOLLOWING",
            SqlKind.Following,
            20,
            null,
            null,
            null);

    /**
     * The PRECEDING operator used exclusively in a window specification.
     */
    static final SqlPostfixOperator precedingOperator =
        new SqlPostfixOperator(
            "PRECEDING",
            SqlKind.Preceding,
            20,
            null,
            null,
            null);

    //~ Constructors -----------------------------------------------------------

    public SqlWindowOperator()
    {
        super("WINDOW", SqlKind.Window, 2, true, null, null, null);
    }

    //~ Methods ----------------------------------------------------------------

    public SqlSyntax getSyntax()
    {
        return SqlSyntax.Special;
    }

    public SqlCall createCall(
        SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands)
    {
        assert functionQualifier == null;
        return new SqlWindow(this, operands, pos);
    }

    public SqlWindow createCall(
        SqlIdentifier declName,
        SqlIdentifier refName,
        SqlNodeList partitionList,
        SqlNodeList orderList,
        boolean isRows,
        SqlParserPos rowRangePos,
        SqlNode lowerBound,
        SqlNode upperBound,
        SqlParserPos pos)
    {
        // If there's only one bound and it's 'FOLLOWING', make it the upper
        // bound.
        if (upperBound == null &&
            lowerBound != null &&
            lowerBound.getKind().equals(SqlKind.Following)) {
            upperBound = lowerBound;
            lowerBound = null;
        }
        return
            (SqlWindow) createCall(
                pos, declName, refName, partitionList, orderList,
                SqlLiteral.createBoolean(isRows, rowRangePos),
                lowerBound, upperBound);
    }

    public <R> void acceptCall(SqlVisitor<R> visitor,
        SqlCall call,
        boolean onlyExpressions,
        SqlBasicVisitor.ArgHandler<R> argHandler)
    {
        if (onlyExpressions) {
            for (int i = 0; i < call.operands.length; i++) {
                SqlNode operand = call.operands[i];

                // if the second parm is an Identifier then it's supposed to
                // be a name from a window clause and isn't part of the
                // group by check
                if (operand == null) {
                    continue;
                }
                if ((i == SqlWindow.RefName_OPERAND)
                    && (operand instanceof SqlIdentifier)) {
                    continue;
                }
                argHandler.visitChild(visitor, call, i, operand);
            }
        } else {
            super.acceptCall(visitor, call, onlyExpressions, argHandler);
        }
    }

    public void unparse(
        SqlWriter writer,
        SqlNode [] operands,
        int leftPrec,
        int rightPrec)
    {
        final SqlWriter.Frame frame =
            writer.startList(SqlWriter.FrameType.Window, "(", ")");
        SqlIdentifier refName =
            (SqlIdentifier) operands[SqlWindow.RefName_OPERAND];
        if (refName != null) {
            refName.unparse(writer, 0, 0);
        }
        SqlNodeList partitionList =
            (SqlNodeList) operands[SqlWindow.PartitionList_OPERAND];
        if (partitionList.size() > 0) {
            writer.sep("PARTITION BY");
            final SqlWriter.Frame partitionFrame = writer.startList("", "");
            partitionList.unparse(writer, 0, 0);
            writer.endList(partitionFrame);
        }
        SqlNodeList orderList =
            (SqlNodeList) operands[SqlWindow.OrderList_OPERAND];
        if (orderList.size() > 0) {
            writer.sep("ORDER BY");
            final SqlWriter.Frame orderFrame = writer.startList("", "");
            orderList.unparse(writer, 0, 0);
            writer.endList(orderFrame);
        }
        boolean isRows =
            SqlLiteral.booleanValue(operands[SqlWindow.IsRows_OPERAND]);
        SqlNode lowerBound = operands[SqlWindow.LowerBound_OPERAND],
            upperBound = operands[SqlWindow.UpperBound_OPERAND];
        if (lowerBound == null) {
            // No ROWS or RANGE clause
        } else if (upperBound == null) {
            if (isRows) {
                writer.sep("ROWS");
            } else {
                writer.sep("RANGE");
            }
            lowerBound.unparse(writer, 0, 0);
        } else {
            if (isRows) {
                writer.sep("ROWS BETWEEN");
            } else {
                writer.sep("RANGE BETWEEN");
            }
            lowerBound.unparse(writer, 0, 0);
            writer.keyword("AND");
            upperBound.unparse(writer, 0, 0);
        }
        writer.endList(frame);
    }

    public void validateCall(
        SqlCall call,
        SqlValidator validator,
        SqlValidatorScope scope,
        SqlValidatorScope operandScope)
    {
        assert call.getOperator() == this;
        final SqlWindow window = (SqlWindow) call;
        final SqlCall windowCall = window.getWindowCall();
        SqlNode [] operands = call.operands;

        //        operandScope = validator.getScope(operands[0]);
        SqlIdentifier refName =
            (SqlIdentifier) operands[SqlWindow.RefName_OPERAND];
        if (refName != null) {
            SqlWindow win = validator.resolveWindow(call, operandScope, false);
            operands = win.operands;
        }

        SqlNodeList partitionList =
            (SqlNodeList) operands[SqlWindow.PartitionList_OPERAND];
        if (null != partitionList) {
            if (0 != partitionList.size()) {
                for (int i = 0; i < partitionList.size(); i++) {
                    SqlNode partitionItem = partitionList.get(i);
                    partitionItem.validateExpr(validator, operandScope);
                }
            } else {
                partitionList = null;
            }
        }

        SqlNodeList orderList =
            (SqlNodeList) operands[SqlWindow.OrderList_OPERAND];
        if (orderList != null) {
            if (0 != orderList.size()) {
                for (int i = 0; i < orderList.size(); i++) {
                    SqlNode orderItem = orderList.get(i);
                    orderItem.validateExpr(validator, scope);
                }
            } else {
                // list is empty so reset the base reference to null so
                // we don't need to keep checking two conditions
                orderList = null;
            }
        }

        boolean isRows =
            SqlLiteral.booleanValue(operands[SqlWindow.IsRows_OPERAND]);
        SqlNode lowerBound = operands[SqlWindow.LowerBound_OPERAND],
            upperBound = operands[SqlWindow.UpperBound_OPERAND];

        boolean triggerFunction = false;
        if (null != windowCall) {
            if (windowCall.isName("RANK") || windowCall.isName("DENSE_RANK")) {
                triggerFunction = true;
            }
        }

        // 6.10 rule 6a Function RANk & DENSE_RANK require OBC
        if ((null == orderList) && triggerFunction && !isTableSorted(scope)) {
            throw validator.newValidationError(
                call,
                EigenbaseResource.instance().FuncNeedsOrderBy.ex());
        }

        // Run framing checks if there are any
        if ((null != upperBound) || (null != lowerBound)) {
            // 6.10 Rule 6a
            if (triggerFunction) {
                throw validator.newValidationError(
                    operands[SqlWindow.IsRows_OPERAND],
                    EigenbaseResource.instance().RankWithFrame.ex());
            }
            SqlTypeFamily orderTypeFam = null;

            // SQL03 7.10 Rule 11a
            if (null != orderList) {
                // if order by is a conpound list then range not allowed
                if ((orderList.size() > 1) && !isRows) {
                    throw validator.newValidationError(
                        operands[SqlWindow.IsRows_OPERAND],
                        EigenbaseResource.instance()
                        .CompoundOrderByProhibitsRange.ex());
                }

                // get the type family for the sort key for Frame Boundary Val.
                RelDataType orderType =
                    validator.deriveType(
                        operandScope,
                        orderList.get(0));
                orderTypeFam =
                    SqlTypeFamily.getFamilyForSqlType(
                        orderType.getSqlTypeName());
            } else {
                // requires an ORDER BY clause if frame is logical(RANGE)
                // We relax this requirment if the table appears to be
                // sorted already
                if (!isRows && !isTableSorted(scope)) {
                    throw validator.newValidationError(
                        call,
                        EigenbaseResource.instance().OverMissingOrderBy.ex());
                }
            }

            // Let the bounds validate themselves
            validateFrameBoundary(lowerBound,
                isRows,
                orderTypeFam,
                validator,
                operandScope);
            validateFrameBoundary(upperBound,
                isRows,
                orderTypeFam,
                validator,
                operandScope);

            // Validate across boundries. 7.10 Rule 8 a-d
            checkSpecialLiterals(window, validator);
        } else if ((null == orderList) && !isTableSorted(scope)) {
            throw validator.newValidationError(
                call,
                EigenbaseResource.instance().OverMissingOrderBy.ex());
        }
    }

    private void validateFrameBoundary(
        SqlNode bound,
        boolean isRows,
        SqlTypeFamily orderTypeFam,
        SqlValidator validator,
        SqlValidatorScope scope)
    {
        if (null == bound) {
            return;
        }
        bound.validate(validator, scope);
        switch (bound.getKind().getOrdinal()) {
        case SqlKind.LiteralORDINAL:

            // is there really anything to validate here? this covers
            // "CURRENT_ROW","unbounded preceding" & "unbounded following"
            break;

        case SqlKind.OtherORDINAL:
        case SqlKind.FollowingORDINAL:
        case SqlKind.PrecedingORDINAL:
            assert (bound instanceof SqlCall);
            final SqlNode boundVal = ((SqlCall) bound).getOperands()[0];

            // Boundries must be a constant
            if (!(boundVal instanceof SqlLiteral)) {
                throw validator.newValidationError(
                    boundVal,
                    EigenbaseResource.instance().RangeOrRowMustBeConstant.ex());
            }

            // SQL03 7.10 rule 11b
            // Physical ROWS must be a numeric constant.
            if (isRows && !(boundVal instanceof SqlNumericLiteral)) {
                throw validator.newValidationError(
                    boundVal,
                    EigenbaseResource.instance().RowMustBeNumeric.ex());
            }

            // if this is a range spec check and make sure the boundery type
            // and order by type are compatible
            if ((null != orderTypeFam) && !isRows) {
                RelDataType bndType = validator.deriveType(scope, boundVal);
                SqlTypeFamily bndTypeFam =
                    SqlTypeFamily.getFamilyForSqlType(
                        bndType.getSqlTypeName());
                switch (orderTypeFam.getOrdinal()) {
                case SqlTypeFamily.Numeric_ordinal:
                    if (SqlTypeFamily.Numeric != bndTypeFam) {
                        throw validator.newValidationError(
                            boundVal,
                            EigenbaseResource.instance().OrderByRangeMismatch
                            .ex());
                    }
                    break;
                case SqlTypeFamily.Date_ordinal:
                case SqlTypeFamily.Time_ordinal:
                case SqlTypeFamily.Timestamp_ordinal:
                    if ((SqlTypeFamily.IntervalDayTime != bndTypeFam)
                        && (SqlTypeFamily.IntervalYearMonth != bndTypeFam)) {
                        throw validator.newValidationError(
                            boundVal,
                            EigenbaseResource.instance().OrderByRangeMismatch
                            .ex());
                    }
                    break;
                default:
                    throw validator.newValidationError(
                        boundVal,
                        EigenbaseResource.instance()
                        .OrderByDataTypeProhibitsRange.ex());
                }
            }
            break;
        default:
            throw Util.newInternal("Unexpected node type");
        }
    }

    private static void checkSpecialLiterals(
        SqlWindow window,
        SqlValidator validator)
    {
        final SqlNode lowerBound = window.getLowerBound();
        final SqlNode upperBound = window.getUpperBound();
        Object lowerLitType = null;
        Object upperLitType = null;
        SqlOperator lowerOp = null;
        SqlOperator upperOp = null;
        if (null != lowerBound) {
            if (lowerBound.getKind().getOrdinal() == SqlKind.LiteralORDINAL) {
                lowerLitType = ((SqlLiteral) lowerBound).getValue();
                if (Bound.UnboundedFollowing == lowerLitType) {
                    throw validator.newValidationError(
                        lowerBound,
                        EigenbaseResource.instance().BadLowerBoundary.ex());
                }
            } else if (lowerBound instanceof SqlCall) {
                lowerOp = ((SqlCall) lowerBound).getOperator();
            }
        }
        if (null != upperBound) {
            if (upperBound.getKind().getOrdinal() == SqlKind.LiteralORDINAL) {
                upperLitType = ((SqlLiteral) upperBound).getValue();
                if (Bound.UnboundedPreceding == upperLitType) {
                    throw validator.newValidationError(
                        upperBound,
                        EigenbaseResource.instance().BadUpperBoundary.ex());
                }
            } else if (upperBound instanceof SqlCall) {
                upperOp = ((SqlCall) upperBound).getOperator();
            }
        }

        if (Bound.CurrentRow == lowerLitType) {
            if (null != upperOp) {
                if (upperOp == precedingOperator) {
                    throw validator.newValidationError(
                        upperBound,
                        EigenbaseResource.instance().CurrentRowPrecedingError
                        .ex());
                }
            }
        } else if (null != lowerOp) {
            if (lowerOp == followingOperator) {
                if (null != upperOp) {
                    if (upperOp == precedingOperator) {
                        throw validator.newValidationError(
                            upperBound,
                            EigenbaseResource.instance()
                            .FollowingBeforePrecedingError.ex());
                    }
                } else if (null != upperLitType) {
                    if (Bound.CurrentRow == upperLitType) {
                        throw validator.newValidationError(
                            upperBound,
                            EigenbaseResource.instance()
                            .CurrentRowFollowingError.ex());
                    }
                }
            }
        }

        // Check that window size is non-negative. I would prefer to allow
        // negative windows and return NULL (as Oracle does) but this is
        // expedient.
        final OffsetRange offsetAndRange =
            getOffsetAndRange(lowerBound, upperBound, false);
        if (offsetAndRange.range < 0) {
            throw validator.newValidationError(
                window,
                EigenbaseResource.instance().WindowHasNegativeSize.ex());
        }
    }

    /**
     * This method retrieves the list of columns for the current table then
     * walks through the list looking for a column that is monotonic (sorted)
     */
    private static boolean isTableSorted(SqlValidatorScope scope)
    {
        List<SqlMoniker> columnNames = new ArrayList<SqlMoniker>();
        scope.findAllColumnNames(null, columnNames);
        for (SqlMoniker columnName : columnNames) {
            SqlIdentifier columnId = columnName.toIdentifier();
            if (scope.isMonotonic(columnId)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Creates a window <code>(RANGE <i>columnName</i> CURRENT ROW)</code>.
     *
     * @param columnName Order column
     */
    public SqlWindow createCurrentRowWindow(final String columnName)
    {
        return
            createCall(
                null,
                null,
                new SqlNodeList(SqlParserPos.ZERO),
                new SqlNodeList(
                    Collections.singletonList(
                        new SqlIdentifier(
                            new String[] { columnName },
                            SqlParserPos.ZERO)),
                    SqlParserPos.ZERO),
                true,
                SqlParserPos.ZERO,
                createCurrentRow(SqlParserPos.ZERO),
                createCurrentRow(SqlParserPos.ZERO),
                SqlParserPos.ZERO);
    }

    /**
     * Creates a window <code>(RANGE <i>columnName</i> UNBOUNDED
     * PRECEDING)</code>.
     *
     * @param columnName Order column
     */
    public SqlWindow createUnboundedPrecedingWindow(final String columnName)
    {
        return
            createCall(
                null,
                null,
                new SqlNodeList(SqlParserPos.ZERO),
                new SqlNodeList(
                    Collections.singletonList(
                        new SqlIdentifier(
                            new String[] { columnName },
                            SqlParserPos.ZERO)),
                    SqlParserPos.ZERO),
                false,
                SqlParserPos.ZERO,
                createUnboundedPreceding(SqlParserPos.ZERO),
                createCurrentRow(SqlParserPos.ZERO),
                SqlParserPos.ZERO);
    }

    public static SqlNode createCurrentRow(SqlParserPos pos)
    {
        return SqlLiteral.createSymbol(Bound.CurrentRow, pos);
    }

    public static SqlNode createUnboundedFollowing(SqlParserPos pos)
    {
        return SqlLiteral.createSymbol(Bound.UnboundedFollowing, pos);
    }

    public static SqlNode createUnboundedPreceding(SqlParserPos pos)
    {
        return SqlLiteral.createSymbol(Bound.UnboundedPreceding, pos);
    }

    public static SqlNode createFollowing(SqlLiteral literal, SqlParserPos pos)
    {
        return followingOperator.createCall(pos, literal);
    }

    public static SqlNode createPreceding(SqlLiteral literal, SqlParserPos pos)
    {
        return precedingOperator.createCall(pos, literal);
    }

    public static SqlNode createBound(SqlLiteral range)
    {
        return range;
    }

    /**
     * Returns whether an expression represents the "CURRENT ROW" bound.
     */
    public static boolean isCurrentRow(SqlNode node)
    {
        return node instanceof SqlLiteral &&
            SqlLiteral.symbolValue(node) == Bound.CurrentRow;
    }

    /**
     * Returns whether an expression represents the "UNBOUNDED PRECEDING"
     * bound.
     */
    public static boolean isUnboundedPreceding(SqlNode node)
    {
        return node instanceof SqlLiteral &&
            SqlLiteral.symbolValue(node) == Bound.UnboundedPreceding;
    }

    /**
     * Returns whether an expression represents the "UNBOUNDED FOLLOWING"
     * bound.
     */
    public static boolean isUnboundedFollowing(SqlNode node)
    {
        return node instanceof SqlLiteral &&
            SqlLiteral.symbolValue(node) == Bound.UnboundedFollowing;
    }

    public static OffsetRange getOffsetAndRange(
        final SqlNode lowerBound,
        final SqlNode upperBound,
        boolean physical)
    {
        ValSign upper =
            getRangeOffset(upperBound, precedingOperator);
        ValSign lower =
            getRangeOffset(lowerBound, followingOperator);
        long offset = upper.signedVal();
        long range = lower.signedVal()  + upper.signedVal();
        if (physical &&
            (lower.sign != upper.sign || lower.val == 0 || upper.val == 0)) {
            ++range;
        }
        return new OffsetRange(offset, range);
    }

    private static ValSign getRangeOffset(SqlNode node, SqlPostfixOperator op)
    {
        if (node == null || !(node instanceof SqlCall)) {
            return new ValSign(0, 1);
        }
        final SqlCall call = (SqlCall) node;
        long sign = call.getOperator() == op
            ? -1
            : 1;
        SqlNode [] operands = call.getOperands();
        assert (operands.length == 1) && (operands[0] != null);
        SqlLiteral operand = (SqlLiteral) operands[0];
        Object obj = operand.getValue();
        long val;
        if (obj instanceof BigDecimal) {
            val = ((BigDecimal) obj).intValue();
        } else if (obj instanceof SqlIntervalLiteral.IntervalValue) {
            val = SqlParserUtil.intervalToMillis(
                (SqlIntervalLiteral.IntervalValue) obj);
        } else {
            val = 0;
        }
        return new ValSign(val, sign);
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * An enumeration of types of bounds in a window: <code>CURRENT ROW</code>,
     * <code>UNBOUNDED PRECEDING</code>, and <code>UNBOUNDED FOLLOWING</code>.
     */
    static class Bound
        extends EnumeratedValues.BasicValue
    {
        public static final Bound CurrentRow = new Bound("CURRENT ROW", 0);
        public static final Bound UnboundedPreceding =
            new Bound("UNBOUNDED PRECEDING", 1);
        public static final Bound UnboundedFollowing =
            new Bound("UNBOUNDED FOLLOWING", 2);

        private Bound(String name, int ordinal)
        {
            super(name, ordinal, null);
        }
    }

    public static class OffsetRange {
        public long offset;
        public long range;

        OffsetRange(long offset, long range) {
            this.offset = offset;
            this.range = range;
        }
    }

    private static class ValSign {
        long val;
        long sign;

        ValSign(long val, long sign) {
            this.val = val;
            this.sign = sign;
            assert sign == 1 || sign == -1;
        }

        long signedVal() {
            return val * sign;
        }
    }
}

// End SqlWindowOperator.java

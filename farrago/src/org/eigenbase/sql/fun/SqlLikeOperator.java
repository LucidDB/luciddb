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

package org.eigenbase.sql.fun;

import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.SqlParserPos;
import org.eigenbase.sql.parser.SqlParserUtil;
import org.eigenbase.sql.type.SqlTypeStrategies;
import org.eigenbase.sql.type.SqlTypeUtil;
import org.eigenbase.util.Util;

import java.util.List;


/**
 * An operator describing the <code>LIKE</code> and
 * <code>SIMILAR</code> operators.
 *
 * <p>Syntax of the two operators:<ul>
 * <li><code>src-value [NOT] LIKE pattern-value
 *     [ESCAPE escape-value]</code></li>
 * <li><code>src-value [NOT] SIMILAR pattern-value
 *     [ESCAPE escape-value]</code></li>
 * </ul>
 *
 * <p><b>NOTE</b> If the <code>NOT</code> clause is present the
 * {@link org.eigenbase.sql.parser.SqlParser parser} will generate
 * a eqvivalent to <code>NOT (src LIKE pattern ...)</code>
 *
 * @author Wael Chatila
 * @since Jan 21, 2004
 * @version $Id$
 **/
public class SqlLikeOperator extends SqlSpecialOperator
{
    //~ Instance fields -------------------------------------------------------

    private final boolean negated;

    //~ Constructors ----------------------------------------------------------

    SqlLikeOperator(
        String name,
        SqlKind kind,
        boolean negated)
    {
        // LIKE is right-associative, because that makes it easier to capture
        // dangling ESCAPE clauses: "a like b like c escape d" becomes
        // "a like (b like c escape d)".
        super(name, kind, 15, false, SqlTypeStrategies.rtiNullableBoolean,
            SqlTypeStrategies.otiFirstKnown,
            SqlTypeStrategies.otcStringSameX3);
        this.negated = negated;
    }

    //~ Methods ---------------------------------------------------------------

    public boolean isNegated()
    {
        return negated;
    }

    public SqlOperandCountRange getOperandCountRange()
    {
        return SqlOperandCountRange.TwoOrThree;
    }

    public boolean checkOperandTypes(
        SqlCallBinding callBinding,
        boolean throwOnFailure)
    {
        switch (callBinding.getOperandCount()) {
        case 2:
            if (!SqlTypeStrategies.otcStringSameX2.
                checkOperandTypes(callBinding, throwOnFailure)) {
                return false;
            }
            break;
        case 3:
            if (!SqlTypeStrategies.otcStringSameX3.
                checkOperandTypes(callBinding, throwOnFailure)) {
                return false;
            }

            //calc implementation should
            //enforce the escape character length to be 1
            break;
        default:
            throw Util.newInternal(
                "unexpected number of args to " + callBinding.getCall());
        }

        if (!SqlTypeUtil.isCharTypeComparable(
            callBinding.getValidator(),
            callBinding.getScope(),
            callBinding.getCall().operands, throwOnFailure)) {
            return false;
        }
        return true;
    }

    public void unparse(
        SqlWriter writer,
        SqlNode [] operands,
        int leftPrec,
        int rightPrec)
    {
        final SqlWriter.Frame frame = writer.startList("", "");
        operands[0].unparse(
            writer, getLeftPrec(), getRightPrec());
        writer.sep(getName());

        operands[1].unparse(writer, getLeftPrec(), getRightPrec());
        if (operands.length == 3) {
            writer.sep("ESCAPE");
            operands[2].unparse(writer, getLeftPrec(), getRightPrec());
        }
        writer.endList(frame);
    }

    public int reduceExpr(
        final int opOrdinal,
        List list)
    {
        // Example:
        //   a LIKE b || c ESCAPE d || e AND f
        // |  |    |      |      |      |
        //  exp0    exp1          exp2
        SqlNode exp0 = (SqlNode) list.get(opOrdinal - 1);
        SqlOperator op = ((SqlParserUtil.ToTreeListItem)
            list.get(opOrdinal)).getOperator();
        assert op instanceof SqlLikeOperator;
        SqlNode exp1 =
            SqlParserUtil.toTreeEx(
                list, opOrdinal + 1, getRightPrec(), SqlKind.Escape);
        SqlNode exp2 = null;
        if ((opOrdinal + 2) < list.size()) {
            final Object o = list.get(opOrdinal + 2);
            if (o instanceof SqlParserUtil.ToTreeListItem) {
                final SqlOperator op2 =
                    ((SqlParserUtil.ToTreeListItem) o).getOperator();
                if (op2.getKind() == SqlKind.Escape) {
                    exp2 =
                        SqlParserUtil.toTreeEx(
                            list, opOrdinal + 3, getRightPrec(),
                            SqlKind.Escape);
                }
            }
        }
        final SqlNode [] operands;
        int end;
        if (exp2 != null) {
            operands = new SqlNode [] { exp0, exp1, exp2 };
            end = opOrdinal + 4;
        } else {
            operands = new SqlNode [] { exp0, exp1 };
            end = opOrdinal + 2;
        }
        SqlCall call = createCall(operands, SqlParserPos.ZERO);
        SqlParserUtil.replaceSublist(list, opOrdinal - 1, end, call);
        return opOrdinal - 1;
    }
}


// End SqlLikeOperator.java

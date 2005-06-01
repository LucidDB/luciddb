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
import org.eigenbase.sql.validate.SqlValidatorScope;
import org.eigenbase.sql.validate.SqlValidator;
import org.eigenbase.sql.parser.SqlParserPos;
import org.eigenbase.sql.parser.SqlParserUtil;
import org.eigenbase.sql.type.*;
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
        super(name, kind, 15, false, ReturnTypeInferenceImpl.useNullableBoolean,
            UnknownParamInference.useFirstKnown,

        /** this is not correct in general */
        OperandsTypeChecking.typeNullableStringStringString);
        this.negated = negated;
    }

    //~ Methods ---------------------------------------------------------------

    public boolean isNegated()
    {
        return negated;
    }
    
    public OperandsCountDescriptor getOperandsCountDescriptor()
    {
        return new OperandsCountDescriptor(2, 3);
    }

    protected boolean checkArgTypes(
        SqlCall call,
        SqlValidator validator,
        SqlValidatorScope scope,
        boolean throwOnFailure)
    {
        switch (call.operands.length) {
        case 2:
            if (!OperandsTypeChecking.typeNullableStringStringOfSameType.
                check(validator, scope, call, throwOnFailure)) {
                return false;
            }
            break;
        case 3:
            if (!OperandsTypeChecking.typeNullableStringStringStringOfSameType.
                check(validator, scope, call, throwOnFailure)) {
                return false;
            }

            //calc implementation should
            //enforce the escape character length to be 1
            break;
        default:
            throw Util.newInternal("unexpected number of args to " + call);
        }

        if (!SqlTypeUtil.isCharTypeComparable(
            validator, scope, call.operands, throwOnFailure)) {
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
        operands[0].unparse(
            writer, getLeftPrec(), getRightPrec());
        writer.print(' ');
        writer.print(getName());
        writer.print(' ');

        operands[1].unparse(writer, getLeftPrec(), getRightPrec());
        if (operands.length == 3) {
            writer.print(" ESCAPE ");
            operands[2].unparse(writer, getLeftPrec(), getRightPrec());
        }
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
        SqlCall call;
        final SqlParserPos pos = null;
        final SqlNode [] operands;
        int end;
        if (exp2 != null) {
            operands = new SqlNode [] { exp0, exp1, exp2 };
            end = opOrdinal + 4;
        } else {
            operands = new SqlNode [] { exp0, exp1 };
            end = opOrdinal + 2;
        }
        call = createCall(operands, pos);
        SqlParserUtil.replaceSublist(list, opOrdinal - 1, end, call);
        return opOrdinal - 1;
    }
}


// End SqlLikeOperator.java

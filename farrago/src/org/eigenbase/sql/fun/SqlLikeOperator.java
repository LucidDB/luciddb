/*
// $Id$
// Package org.eigenbase is a class library of database components.
// Copyright (C) 2002-2004 Disruptive Tech
// Copyright (C) 2003-2004 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
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

import java.util.ArrayList;
import java.util.List;

import org.eigenbase.sql.*;
import org.eigenbase.sql.validation.ValidationUtil;
import org.eigenbase.sql.type.UnknownParamInference;
import org.eigenbase.sql.type.ReturnTypeInference;
import org.eigenbase.sql.type.OperandsTypeChecking;
import org.eigenbase.sql.parser.ParserPosition;
import org.eigenbase.sql.parser.ParserUtil;
import org.eigenbase.util.Util;


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
public abstract class SqlLikeOperator extends SqlSpecialOperator
{
    //~ Instance fields -------------------------------------------------------

    public final boolean negated;

    //~ Constructors ----------------------------------------------------------

    SqlLikeOperator(
        String name,
        SqlKind kind,
        boolean negated)
    {
        // LIKE is right-associative, because that makes it easier to capture
        // dangling ESCAPE clauses: "a like b like c escape d" becomes
        // "a like (b like c escape d)".
        super(name, kind, 15, false, ReturnTypeInference.useNullableBoolean,
            UnknownParamInference.useFirstKnown,
            
        /** this is not correct in general */
        OperandsTypeChecking.typeNullableStringStringString);
        this.negated = negated;
    }

    //~ Methods ---------------------------------------------------------------

    public OperandsCountDescriptor getOperandsCountDescriptor()
    {
        return new OperandsCountDescriptor(2, 3);
    }

    protected void checkArgTypes(
        SqlCall call,
        SqlValidator validator,
        SqlValidator.Scope scope)
    {
        switch (call.operands.length) {
        case 2:
            OperandsTypeChecking.typeNullableStringStringOfSameType.check(validator,
                scope, call, true);
            break;
        case 3:
            OperandsTypeChecking.typeNullableStringStringStringOfSameType.check(validator,
                scope, call, true);

            //calc implementation should
            //enforce the escape character length to be 1
            break;
        default:
            throw Util.newInternal("unexpected number of args to " + call);
        }

        ValidationUtil.isCharTypeComparableThrows(validator, scope, call.operands);
    }

    public void unparse(
        SqlWriter writer,
        SqlNode [] operands,
        int leftPrec,
        int rightPrec)
    {
        operands[0].unparse(writer, this.leftPrec, this.rightPrec);
        writer.print(' ');
        writer.print(name);
        writer.print(' ');

        operands[1].unparse(writer, this.leftPrec, this.rightPrec);
        if (operands.length == 3) {
            writer.print(" ESCAPE ");
            operands[2].unparse(writer, this.leftPrec, this.rightPrec);
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
        SqlOperator op = ((ParserUtil.ToTreeListItem) list.get(opOrdinal)).op;
        assert op instanceof SqlLikeOperator;
        SqlNode exp1 =
            ParserUtil.toTreeEx(list, opOrdinal + 1, rightPrec, SqlKind.Escape);
        SqlNode exp2 = null;
        if ((opOrdinal + 2) < list.size()) {
            final Object o = list.get(opOrdinal + 2);
            if (o instanceof ParserUtil.ToTreeListItem) {
                final SqlOperator op2 = ((ParserUtil.ToTreeListItem) o).op;
                if (op2.kind == SqlKind.Escape) {
                    exp2 =
                        ParserUtil.toTreeEx(list, opOrdinal + 3, rightPrec,
                            SqlKind.Escape);
                }
            }
        }
        SqlCall call;
        final ParserPosition pos = null;
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
        ParserUtil.replaceSublist(list, opOrdinal - 1, end, call);
        return opOrdinal - 1;
    }
}


// End SqlLikeOperator.java

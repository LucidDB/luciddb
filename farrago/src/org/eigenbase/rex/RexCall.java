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

package org.eigenbase.rex;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.sql.SqlKind;
import org.eigenbase.sql.SqlOperator;
import org.eigenbase.sql.SqlSyntax;


/**
 * An expression formed by a call to an operator with zero or more expressions
 * as operands.
 *
 * <p>Operators may be binary, unary, functions, special syntactic constructs
 * like <code>CASE ... WHEN ... END</code>, or even internally generated
 * constructs like implicit type conversions. The syntax of the operator is
 * really irrelevant, because row-expressions (unlike
 * {@link org.eigenbase.sql.SqlNode SQL expressions}) do not directly represent
 * a piece of source code.</p>
 *
 * <p>It's not often necessary to sub-class this class. The smarts should be
 * in the operator, rather than the call. Any extra information about the call
 * can often be encoded as extra arguments. (These don't need to be hidden,
 * because no one is going to be generating source code from this tree.)</p>
 *
 * @author jhyde
 * @since Nov 24, 2003
 * @version $Id$
 **/
public class RexCall extends RexNode
{
    //~ Instance fields -------------------------------------------------------

    public final SqlOperator op;
    public final RexNode [] operands;
    private final RelDataType type;
    private final RexKind kind;

    //~ Constructors ----------------------------------------------------------

    RexCall(
        RelDataType type,
        SqlOperator op,
        RexNode [] operands)
    {
        assert type != null : "precondition: type != null";
        assert op != null : "precondition: op != null";
        assert operands != null : "precondition: operands != null";
        this.type = type;
        this.op = op;
        this.operands = operands;
        this.kind = sqlKindToRexKind(op.kind);
        assert this.kind != null : op;
        this.digest = computeDigest(true);
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * Returns the {@link RexKind} corresponding to a {@link SqlKind}.
     * Fails if there is none.
     *
     * @post return != null
     */
    static RexKind sqlKindToRexKind(SqlKind kind)
    {
        switch (kind.getOrdinal()) {
        case SqlKind.EqualsORDINAL:
            return RexKind.Equals;
        case SqlKind.IdentifierORDINAL:
            return RexKind.Identifier;
        case SqlKind.LiteralORDINAL:
            return RexKind.Literal;
        case SqlKind.DynamicParamORDINAL:
            return RexKind.DynamicParam;
        case SqlKind.TimesORDINAL:
            return RexKind.Times;
        case SqlKind.DivideORDINAL:
            return RexKind.Divide;
        case SqlKind.PlusORDINAL:
            return RexKind.Plus;
        case SqlKind.MinusORDINAL:
            return RexKind.Minus;
        case SqlKind.LessThanORDINAL:
            return RexKind.LessThan;
        case SqlKind.GreaterThanORDINAL:
            return RexKind.GreaterThan;
        case SqlKind.LessThanOrEqualORDINAL:
            return RexKind.LessThanOrEqual;
        case SqlKind.GreaterThanOrEqualORDINAL:
            return RexKind.GreaterThanOrEqual;
        case SqlKind.NotEqualsORDINAL:
            return RexKind.NotEquals;
        case SqlKind.OrORDINAL:
            return RexKind.Or;
        case SqlKind.AndORDINAL:
            return RexKind.And;
        case SqlKind.NotORDINAL:
            return RexKind.Not;
        case SqlKind.IsTrueORDINAL:
            return RexKind.IsTrue;
        case SqlKind.IsFalseORDINAL:
            return RexKind.IsFalse;
        case SqlKind.IsNullORDINAL:
            return RexKind.IsNull;
        case SqlKind.IsUnknownORDINAL:
            return RexKind.IsNull;
        case SqlKind.PlusPrefixORDINAL:
            return RexKind.Plus;
        case SqlKind.MinusPrefixORDINAL:
            return RexKind.MinusPrefix;
        case SqlKind.ValuesORDINAL:
            return RexKind.Values;
        case SqlKind.RowORDINAL:
            return RexKind.Row;
        case SqlKind.CastORDINAL:
            return RexKind.Cast;
        case SqlKind.TrimORDINAL:
            return RexKind.Trim;
        case SqlKind.FunctionORDINAL:
            return RexKind.Other;
        case SqlKind.CaseORDINAL:
            return RexKind.Other;
        case SqlKind.OtherORDINAL:
            return RexKind.Other;
        case SqlKind.LikeORDINAL:
            return RexKind.Like;
        case SqlKind.SimilarORDINAL:
            return RexKind.Similar;
        default:
            throw kind.unexpected();
        }
    }

    protected String computeDigest(boolean withType)
    {
        StringBuffer sb = new StringBuffer(op.name);
        if (operands.length == 0
            && op.getSyntax() == SqlSyntax.FunctionId) {
            // Don't print params for empty arg list. For example, we want
            // "SYSTEM_USER", not "SYSTEM_USER()".
        } else {
            sb.append("(");
            for (int i = 0; i < operands.length; i++) {
                if (i > 0) {
                    sb.append(", ");
                }
                RexNode operand = operands[i];
                sb.append(operand.toString());
            }
            sb.append(")");
        }
        if (withType) {
            sb.append(":");
            sb.append(type.toString());
        }
        return sb.toString();
    }

    public String toString()
    {
        return computeDigest(false);
    }

    public void accept(RexVisitor visitor)
    {
        visitor.visitCall(this);
    }

    public RelDataType getType()
    {
        return type;
    }

    public Object clone()
    {
        return new RexCall(
            type,
            op,
            RexUtil.clone(operands));
    }

    public RexKind getKind()
    {
        return kind;
    }

    public RexNode [] getOperands()
    {
        return operands;
    }

    public SqlOperator getOperator()
    {
        return op;
    }
}


// End RexCall.java

/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
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
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.validate.*;

import java.math.BigDecimal;


/**
 * Base class for binary operators such as addition, subtraction, and
 * multiplication which are monotonic for the patterns <code>m op c</code> and
 * <code>c op m</code> where m is any monotonic expression and c is a constant.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class SqlMonotonicBinaryOperator
    extends SqlBinaryOperator
{
    //~ Constructors -----------------------------------------------------------

    public SqlMonotonicBinaryOperator(
        String name,
        SqlKind kind,
        int prec,
        boolean isLeftAssoc,
        SqlReturnTypeInference returnTypeInference,
        SqlOperandTypeInference operandTypeInference,
        SqlOperandTypeChecker operandTypeChecker)
    {
        super(
            name,
            kind,
            prec,
            isLeftAssoc,
            returnTypeInference,
            operandTypeInference,
            operandTypeChecker);
    }

    //~ Methods ----------------------------------------------------------------

    public SqlMonotonicity getMonotonicity(
        SqlCall call,
        SqlValidatorScope scope)
    {
        final SqlMonotonicity mono0 = scope.getMonotonicity(call.operands[0]);
        final SqlMonotonicity mono1 = scope.getMonotonicity(call.operands[1]);

        // constant <op> constant --> constant
        if (mono1 == SqlMonotonicity.Constant
            && mono0 == SqlMonotonicity.Constant)
        {
            return SqlMonotonicity.Constant;
        }

        // monotonic <op> constant
        if (mono1 == SqlMonotonicity.Constant) {
            // mono0 + constant --> mono0
            // mono0 - constant --> mono0
            if (getName().equals("-")
                || getName().equals("+")) {
                return mono0;
            }
            assert getName().equals("*");
            if (call.operands[1] instanceof SqlLiteral) {
                SqlLiteral literal = (SqlLiteral) call.operands[1];
                switch (literal.bigDecimalValue().compareTo(
                    BigDecimal.ZERO)) {
                case -1:
                    // mono0 * negative constant --> reverse mono0
                    return mono0.reverse();
                case 0:
                    // mono0 * 0 --> constant (zero)
                    return SqlMonotonicity.Constant;
                default:
                    // mono0 * positiove constant --> mono0
                    return mono0;
                }
            }
            return mono0;
        }

        // constant <op> mono
        if (mono0 == SqlMonotonicity.Constant) {
            if (getName().equals("-")) {
                // constant - mono1 --> reverse mono1
                return mono1.reverse();
            }
            if (getName().equals("+")) {
                // constant + mono1 --> mono1
                return mono1;
            }
            assert getName().equals("*");
            if (call.operands[0] instanceof SqlLiteral) {
                SqlLiteral literal = (SqlLiteral) call.operands[0];
                switch (literal.bigDecimalValue().compareTo(
                    BigDecimal.ZERO)) {
                case -1:
                    // negative constant * mono1 --> reverse mono1
                    return mono1.reverse();
                case 0:
                    // 0 * mono1 --> constant (zero)
                    return SqlMonotonicity.Constant;
                default:
                    // positive constant * mono1 --> mono1
                    return mono1;
                }
            }
        }

        // strictly asc + strictly asc --> strictly asc
        // asc + asc --> asc
        // asc + desc --> not monotonic
        assertMonotonicity("2 * orderid + 3 * orderid", SqlMonotonicity.StrictlyIncreasing);
        assertMonotonicity("2 * orderid + (-3 * orderid)", SqlMonotonicity.NotMonotonic);
        assertMonotonicity("2 * orderid + 3 * orderid", SqlMonotonicity.StrictlyIncreasing);

        if (getName().equals("+")) {
            if (mono0 == mono1) {
                return mono0;
            } else if (mono0.unstrict() == mono1.unstrict()) {
                return mono0.unstrict();
            } else {
                return SqlMonotonicity.NotMonotonic;
            }
        }
        if (getName().equals("-")) {
            if (mono0 == mono1.reverse()) {
                return mono0;
            } else if (mono0.unstrict() == mono1.reverse().unstrict()) {
                return mono0.unstrict();
            } else {
                return SqlMonotonicity.NotMonotonic;
            }
        }
        if (getName().equals("*")) {
            return SqlMonotonicity.NotMonotonic;
        }

        return super.getMonotonicity(call, scope);
    }


    private void assertMonotonicity(
        String s, SqlMonotonicity monotonicity)
    {
        //To change body of created methods use File | Settings | File Templates.
    }


}

// End SqlMonotonicBinaryOperator.java

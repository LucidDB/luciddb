/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2004-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
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
import org.eigenbase.sql.test.SqlOperatorTests;
import org.eigenbase.sql.test.SqlTester;
import org.eigenbase.sql.type.OperandsTypeChecking;
import org.eigenbase.sql.type.ReturnTypeInferenceImpl;
import org.eigenbase.sql.type.SqlTypeUtil;
import org.eigenbase.sql.validate.SqlValidatorScope;
import org.eigenbase.sql.validate.SqlValidator;
import org.eigenbase.util.EnumeratedValues;

/**
 * Definition of the "TRIM" builtin SQL function.
 *
 * @author Wael Chatila, Julian Hyde
 * @since May 28, 2004
 * @version $Id$
 **/
public class SqlTrimFunction extends SqlFunction
{
    //~ Constructors ----------------------------------------------------------

    public SqlTrimFunction()
    {
        super("TRIM", SqlKind.Trim,
            new ReturnTypeInferenceImpl.TransformCascade(
                ReturnTypeInferenceImpl.useThirdArgType,
                ReturnTypeInferenceImpl.toNullable
            ),
            null,
            OperandsTypeChecking.typeNullableStringStringOfSameType,
            SqlFunctionCategory.String);
    }

    //~ Methods ---------------------------------------------------------------

    public OperandsCountDescriptor getOperandsCountDescriptor()
    {
        return new OperandsCountDescriptor(3);
    }

    public void unparse(
        SqlWriter writer,
        SqlNode [] operands,
        int leftPrec,
        int rightPrec)
    {
        writer.print(name);
        writer.print("(");
        assert operands[0] instanceof SqlLiteral;
        operands[0].unparse(writer, 0, 0);
        writer.print(" ");
        operands[1].unparse(writer, leftPrec, rightPrec);
        writer.print(" FROM ");
        operands[2].unparse(writer, leftPrec, rightPrec);
        writer.print(")");
    }

    protected String getSignatureTemplate(final int operandsCount)
    {
        switch (operandsCount) {
        case 2:
            return "{0}({1} FROM {2})";
        case 3:
            return "{0}({1} {2} FROM {3})";
        }
        assert (false);
        return null;
    }

    public SqlCall createCall(
        SqlNode [] operands,
        SqlParserPos pos)
    {
        assert (3 == operands.length);
        if (null == operands[0]) {
            operands[0] = SqlLiteral.createSymbol(Flag.Both, pos);
        }

        if (null == operands[1]) {
            operands[1] = SqlLiteral.createCharString(" ", pos);
        }
        return super.createCall(operands, pos);
    }

    protected boolean checkArgTypes(
        SqlCall call,
        SqlValidator validator,
        SqlValidatorScope scope,
        boolean throwOnFailure)
    {
        for (int i = 1; i < 3; i++) {
            if (!OperandsTypeChecking.typeNullableString.check(call, validator,
                        scope, call.operands[i], 0, throwOnFailure)) {
                if (throwOnFailure) {
                    throw call.newValidationSignatureError(validator, scope);
                }
                return false;
            }
        }

        SqlNode [] ops = new SqlNode[2];
        for (int i = 1; i < call.operands.length; i++) {
            ops[i - 1] = call.operands[i];
        }

        return SqlTypeUtil.isCharTypeComparable(
            validator, scope, ops, throwOnFailure);
    }

    public void test(SqlTester tester)
    {
        SqlOperatorTests.testTrimFunc(tester);
    }

    //~ Inner Classes ---------------------------------------------------------

    /**
     * Defines the enumerated values "LEADING", "TRAILING", "BOTH".
     */
    public static class Flag extends EnumeratedValues.BasicValue
    {
        public final int left;
        public final int right;

        private Flag(String name,
            int left,
            int right, int ordinal)
        {
            super(name, ordinal, null);
            this.left = left;
            this.right = right;
        }

        public static final int Both_ordinal = 0;
        public static final Flag Both = new Flag("Both", 1, 1, Both_ordinal);
        public static final int Leading_ordinal = 1;
        public static final Flag Leading = new Flag("Leading", 1, 0, Leading_ordinal);
        public static final int Trailing_ordinal = 2;
        public static final Flag Trailing = new Flag("Trailing", 0, 1, Trailing_ordinal);
        public static final EnumeratedValues enumeration =
            new EnumeratedValues(new Flag[] {Both, Leading, Trailing});
    }
}

// End SqlTrimFunction.java

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

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.sql.*;
import org.eigenbase.sql.type.OperandsTypeChecking;
import org.eigenbase.sql.validation.ValidationUtil;
import org.eigenbase.sql.parser.ParserPosition;
import org.eigenbase.sql.test.SqlTester;


/**
 * Definition of the "TRIM" builtin SQL function.
 *
 * @author jhyde
 * @since May 28, 2004
 * @version $Id$
 **/
public class SqlTrimFunction extends SqlFunction
{
    //~ Constructors ----------------------------------------------------------

    public SqlTrimFunction()
    {
        super("TRIM", SqlKind.Trim, null, null,
            OperandsTypeChecking.typeNullableStringStringOfSameType,
            SqlFunction.SqlFuncTypeName.String);
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
        assert (operands[0] instanceof Flag);
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
        ParserPosition pos)
    {
        assert (3 == operands.length);
        if (null == operands[0]) {
            operands[0] = Flag.createBoth(pos);
        }

        if (null == operands[1]) {
            operands[1] = SqlLiteral.CharString.create(" ", pos);
        }
        return super.createCall(operands, pos);
    }

    protected void checkArgTypes(
        SqlCall call,
        SqlValidator validator,
        SqlValidator.Scope scope)
    {
        for (int i = 1; i < 3; i++) {
            if (!OperandsTypeChecking.typeNullableString.check(call, validator,
                        scope, call.operands[i], 0, true)) {
                throw call.newValidationSignatureError(validator, scope);
            }
        }
    }

    public RelDataType getType(
        RelDataTypeFactory typeFactory,
        RelDataType [] argTypes)
    {
        assert (3 == argTypes.length);
        return ValidationUtil.makeNullableIfOperandsAre(typeFactory,
            argTypes, argTypes[2]);
    }

    public RelDataType getType(
        SqlValidator validator,
        SqlValidator.Scope scope,
        SqlCall call)
    {
        checkArgTypes(call, validator, scope);

        SqlNode [] ops = new SqlNode[2];
        for (int i = 1; i < call.operands.length; i++) {
            ops[i - 1] = call.operands[i];
        }

        ValidationUtil.isCharTypeComparableThrows(validator, scope, ops);
        RelDataType type = validator.deriveType(scope, call.operands[2]);
        return ValidationUtil.makeNullableIfOperandsAre(validator, scope,
            call, type);
    }

    public void test(SqlTester tester)
    {
        tester.checkString("trim('a' from 'aAa')", "A");
        tester.checkString("trim(both 'a' from 'aAa')", "A");
        tester.checkString("trim(leading 'a' from 'aAa')", "Aa");
        tester.checkString("trim(trailing 'a' from 'aAa')", "aA");
        tester.checkNull("trim(cast(null as varchar) from 'a')");
        tester.checkNull("trim('a' from cast(null as varchar))");
    }

    //~ Inner Classes ---------------------------------------------------------

    /**
     * Enumerates the types of flags
     */
    public static class Flag extends SqlSymbol
    {
        public final int left;
        public final int right;

        private Flag(
            String name,
            int left,
            int right,
            ParserPosition pos)
        {
            super(name, pos);
            this.left = left;
            this.right = right;
        }

        public static final SqlSymbol createBoth(
            ParserPosition pos)
        {
            return new Flag("Both", 1, 1, pos);
        }

        public static final SqlSymbol createLeading(
            ParserPosition pos)
        {
            return new Flag("Leading", 1, 0, pos);
        }

        public static final SqlSymbol createTrailing(
            ParserPosition pos)
        {
            return new Flag("Trailing", 0, 1, pos);
        }
    }
}


// End SqlTrimFunction.java

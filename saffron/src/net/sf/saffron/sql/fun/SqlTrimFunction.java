/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2004-2004 Disruptive Technologies, Inc.
// You must accept the terms in LICENSE.html to use this software.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/
package net.sf.saffron.sql.fun;

import net.sf.saffron.core.SaffronType;
import net.sf.saffron.core.SaffronTypeFactory;
import net.sf.saffron.sql.*;
import net.sf.saffron.sql.test.SqlTester;
import net.sf.saffron.util.EnumeratedValues;

import java.util.ArrayList;
import java.util.List;

/**
 * Definition of the "TRIM" builtin SQL function.
 *
 * @author jhyde
 * @since May 28, 2004
 * @version $Id$
 **/
public class SqlTrimFunction extends SqlFunction {
    public SqlTrimFunction() {
        super("TRIM", SqlKind.Trim, null, null,
                SqlOperatorTable.typeNullableStringStringOfSameType,
                SqlFunction.SqlFuncTypeName.String);
    }

    public int getNumOfOperands(int disiredCount) {
        return 3;
    }

    public List getPossibleNumOfOperands() {
        List ret = new ArrayList(argTypeInference.getArgCount());
        ret.add(new Integer(getNumOfOperands(0)));
        return ret;
    }

    public void unparse(SqlWriter writer, SqlNode[] operands,
            int leftPrec, int rightPrec) {
        writer.print(name);
        writer.print("(");
        assert(operands[0] instanceof Flag);
        operands[0].unparse(writer, 0, 0);
        writer.print(" ");
        operands[1].unparse(writer, leftPrec, rightPrec);
        writer.print(" FROM ");
        operands[2].unparse(writer, leftPrec, rightPrec);
        writer.print(")");
    }

    public SqlCall createCall(SqlNode[] operands) {
        assert(3 == operands.length);
        if (null == operands[0]) {
            operands[0] = Flag.Both;
        }

        if (null == operands[1]) {
            operands[1] = SqlLiteral.createString("' '", null);
        }
        return super.createCall(operands);
    }

    protected void checkArgTypes(SqlCall call, SqlValidator validator,
            SqlValidator.Scope scope) {
        for (int i = 1; i < 3; i++) {
            if (!SqlOperatorTable.typeNullableString.check(
                    call,validator, scope, call.operands[i], 0)) {
                throw call.newValidationSignatureError(
                        validator, scope);
            }
        }
    }


    public SaffronType getType(SaffronTypeFactory typeFactory,
            SaffronType[] argTypes) {
        assert(3 == argTypes.length);
        return SqlOperatorTable.makeNullableIfOperandsAre(
                typeFactory,argTypes, argTypes[2]);
    }

    public SaffronType getType(SqlValidator validator,
            SqlValidator.Scope scope, SqlCall call) {
        checkArgTypes(call, validator, scope);

        SqlNode[] ops = new SqlNode[2];
        for (int i = 1; i < call.operands.length; i++) {
            ops[i - 1] = call.operands[i];
        }

        isCharTypeComparableThrows(validator, scope, ops);
        SaffronType type = validator.deriveType(scope, call.operands[2]);
        return SqlOperatorTable.makeNullableIfOperandsAre(
                validator,scope,call,type);
    }

    public void test(SqlTester tester) {
        tester.checkString("trim('a' from 'aAa'", "'A'");
        tester.checkString("trim(both 'a' from 'aAa'", "'A'");
        tester.checkString("trim(leading 'a' from 'aAa'", "'Aa'");
        tester.checkString("trim(trailing 'a' from 'aAa'", "'aA'");
        tester.checkNull("trim(cast(null as varchar) from 'a'");
        tester.checkNull("trim('a' from cast(null as varchar))");
    }

    /**
     * Enumerates the types of flags
     */
    public static class Flag extends SqlSymbol {
        public final int _left;
        public final int _right;

        private Flag(String name, int ordinal, int left, int right) {
            super(name, ordinal);
            _left = left;
            _right = right;
        }

        public static final int Both_ordinal = 0;
        public static final SqlSymbol Both =
                new Flag("Both", Both_ordinal, 1, 1);
        public static final int Leading_ordinal = 1;
        public static final SqlSymbol Leading =
                new Flag("Leading", Leading_ordinal, 1, 0);
        public static final int Trailing_ordinal = 2;
        public static final SqlSymbol Trailing =
                new Flag("Trailing", Trailing_ordinal, 0, 1);

        /**
         * List of all allowable {@link Flag} values.
         */
        public static final EnumeratedValues enumeration = new EnumeratedValues(
                new SqlSymbol[]{Both, Leading, Trailing});
        /**
         * Looks up a flag from its ordinal.
         */
        public static Flag get(int ordinal) {
            return (Flag) enumeration.getValue(ordinal);
        }
        /**
         * Looks up a flag from its name.
         */
        public static Flag get(String name) {
            return (Flag) enumeration.getValue(name);
        }
    }
}

// End SqlTrimFunction.java

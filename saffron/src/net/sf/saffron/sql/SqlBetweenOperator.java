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
*/package net.sf.saffron.sql;

import net.sf.saffron.sql.test.SqlTester;
import net.sf.saffron.core.SaffronType;
import net.sf.saffron.core.SaffronTypeFactory;
import net.sf.saffron.util.EnumeratedValues;

import java.util.List;
import java.util.ArrayList;

/**
 * Defines the between operator.<br>
 * Syntax:<br>
 * <code>X [NOT] BETWEEN [ASSYMETRIC | SYMMETRIC] Y AND Z</code><br>
 * if the assymetric/symmeteric keywords are left out ASSYMETRIC is default
 *
 * @author Wael Chatila 
 * @since Jun 9, 2004
 * @version $Id$
 */
public abstract class SqlBetweenOperator extends SqlSpecialOperator {
    public SqlBetweenOperator(String name, SqlKind kind) {
        super(name, kind, 15, true,
                SqlOperatorTable.useNullableBoolean,
                null,
                null);
    }

    protected void checkArgTypes(SqlCall call, SqlValidator validator,
            SqlValidator.Scope scope) {
        SqlOperatorTable.typeNullableNumeric.check(call,validator,scope,call.operands[0],0);
        SqlOperatorTable.typeNullableNumeric.check(call,validator,scope,call.operands[2],0);
        SqlOperatorTable.typeNullableNumeric.check(call,validator,scope,call.operands[3],0);
    }

    public int getNumOfOperands(int desiredCount) {
        return 4;
    }

    public void unparse(
            SqlWriter writer,
            SqlNode[] operands,
            int leftPrec,
            int rightPrec) {
        operands[0].unparse(writer, this.leftPrec, this.rightPrec);
        writer.print(" "+name);
        if (((SqlBetweenOperator.Flag) operands[1]).isAsymmetric) {
            writer.print(" ASYMMETRIC ");
        } else {
            writer.print(" SYMMETRIC ");
        }
        operands[2].unparse(writer, this.leftPrec, this.rightPrec);
        writer.print(" AND ");
        operands[3].unparse(writer, this.leftPrec, this.rightPrec);
    }

    public static class Flag extends SqlSymbol {
        public final boolean isAsymmetric;

        private Flag(String name, int ordinal, boolean isAsymmetric) {
            super(name, ordinal);
            this.isAsymmetric = isAsymmetric;
        }

        public static final int Asymmetric_ordinal = 0;
        public static final SqlSymbol Asymmetric =
                new Flag("Assymetric", Asymmetric_ordinal, true);

        public static final int Symmetric_ordinal = 1;
        public static final SqlSymbol Symmetric =
                new Flag("Symmetric", Symmetric_ordinal, false);

        /**
         * List of all allowable {@link net.sf.saffron.sql.fun.SqlTrimFunction.Flag} values.
         */
        public static final EnumeratedValues enumeration = new EnumeratedValues(
                new SqlSymbol[]{Asymmetric, Symmetric});
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

// End SqlBetweenOperator.java


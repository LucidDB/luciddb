/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2004-2004 Disruptive Tech
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
package net.sf.saffron.sql;

import net.sf.saffron.sql.test.SqlTester;

/**
 * A generalization of a binary operator to involve several (two or more)
 * arguments, and keywords between each pair of arguments.
 *
 * <p>For example, the <code>BETWEEN</code> operator is ternary, and has syntax
 * <code><i>exp1</i> BETWEEN <i>exp2</i> AND <i>exp3</i></code>.
 *
 * @author jhyde
 * @since Aug 8, 2004
 * @version $Id$
 **/
public class SqlInfixOperator extends SqlSpecialOperator {
    private final String[] names;

    // @pre paramTypes != null
    SqlInfixOperator(
            String[] names, SqlKind kind, int precedence,
            SqlOperator.TypeInference typeInference,
            SqlOperator.ParamTypeInference paramTypeInference,
            SqlOperator.AllowedArgInference argTypeInference) {
        super(names[0], kind, precedence, true, typeInference,
                paramTypeInference, argTypeInference);
        assert names.length > 1;
        this.names = names;
    }

    public SqlSyntax getSyntax() {
        return SqlSyntax.Special;
    }

    public void unparse(
            SqlWriter writer,
            SqlNode[] operands,
            int leftPrec,
            int rightPrec) {
        assert operands.length == names.length + 1;
        for (int i = 0; i < operands.length; i++) {
            if (i > 0) {
                if (needsSpace()) {
                    writer.print(' ');
                    writer.print(names[i - 1]);
                    writer.print(' ');
                } else {
                    writer.print(names[i - 1]);
                }
            }
            operands[i].unparse(writer, leftPrec, this.leftPrec);
        }
    }

    boolean needsSpace() {
        return true;
    }

    public void test(SqlTester tester) {
    }
}

// End SqlInfixOperator.java

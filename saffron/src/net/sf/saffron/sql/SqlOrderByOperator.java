/*
// $Id$
// Saffron preprocessor and data engine
// Copyright (C) 2002-2004 Disruptive Technologies, Inc.
// (C) Copyright 2003-2004 John V. Sichi
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
 * SqlOrderByOperator is used to represent an ORDER BY on a query
 * other than a SELECT (e.g. VALUES or UNION).  It is a purely syntactic
 * operator, and is eliminated by SqlValidator.createInternalSelect
 * and replaced with the ORDER_OPERAND of SqlSelect.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class SqlOrderByOperator extends SqlSpecialOperator
{
    // constants representing operand positions
    public static final int QUERY_OPERAND = 0;
    public static final int ORDER_OPERAND = 1;
    
    public SqlOrderByOperator()
    {
        // NOTE:  make precedence lower then SELECT to avoid extra parens
        super("ORDER BY",SqlKind.OrderBy,0);
    }

    public SqlSyntax getSyntax()
    {
        return SqlSyntax.Postfix;
    }

    public void unparse(
        SqlWriter writer,
        SqlNode [] operands,
        int leftPrec,
        int rightPrec)
    {
        assert(operands.length == 2);
        operands[QUERY_OPERAND].unparse(writer,this.leftPrec,this.rightPrec);
        writer.print(' ');
        writer.print(name);
        writer.print(' ');
        operands[ORDER_OPERAND].unparse(writer,0,0);
    }

    public void test(SqlTester tester) {
        /* empty implementation */
    }
}

// End SqlOrderByOperator.java

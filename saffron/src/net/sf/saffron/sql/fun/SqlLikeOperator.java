/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2004 Disruptive Technologies, Inc.
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

import net.sf.saffron.sql.test.SqlTester;
import net.sf.saffron.sql.*;

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
 * {@link net.sf.saffron.sql.parser.SqlParser parser} will generate
 * a eqvivalent to <code>NOT (src LIKE pattern ...)</code>
 *
 * @author Wael Chatila
 * @since Jan 21, 2004
 * @version $Id$
 **/
public abstract class SqlLikeOperator extends SqlSpecialOperator
{
    SqlLikeOperator(String name, SqlKind kind)
    {
        super(name,kind,15,true,
                SqlOperatorTable.useBoolean,
                SqlOperatorTable.useFirstKnownParam,
                /** this is not correct in general */
                SqlOperatorTable.typeNullableStringStringString);
    }

    public void unparse(SqlWriter writer,
            SqlNode[] operands,
            int leftPrec,
            int rightPrec)
    {
        operands[0].unparse(writer, this.leftPrec, this.rightPrec);
        writer.print(' ');
        writer.print(name);
        writer.print(' ');

        operands[1].unparse(writer, this.leftPrec, this.rightPrec);
        if (operands.length == 3)
        {
            writer.print(" ESCAPE ");
            operands[2].unparse(writer, this.leftPrec, this.rightPrec);
        }
    }

}

// End SqlLikeOperator.java

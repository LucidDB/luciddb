/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2004-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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

import org.eigenbase.sql.SqlInternalOperator;
import org.eigenbase.sql.SqlKind;
import org.eigenbase.sql.SqlWriter;
import org.eigenbase.sql.SqlNode;
import org.eigenbase.sql.type.*;

/**
 * An internal operator that throws an exception. <br>
 * The exception is thrown with a (localized) error message which is the only
 * input paramter to the operator.<br>
 * The return type is defined as a <code>BOOLEAN</code> to facilitate
 * the use of it in constructs like the following:<p>
 * <code>
 * CASE<br>
 *  WHEN &lt;conditionn&gt; THEN true<br>
 *  ELSE throw("what's wrong with you man?")<br>
 * END
 *
 * @author Wael Chatila 
 * @since Mar 29, 2005
 * @version $Id$
 */
public class SqlThrowOperator extends SqlInternalOperator
{
    public SqlThrowOperator()
    {
        super("$throw", SqlKind.Other, 1, true,
            SqlTypeStrategies.rtiBoolean,
            null,
            SqlTypeStrategies.otcNullableVarchar);
    }

    public void unparse(
        SqlWriter writer,
        SqlNode[] operands,
        int leftPrec,
        int rightPrec)
    {
        writer.print(getName());
        writer.print("(");
        operands[0].unparse(writer, 0, 0);
        writer.print(")");
    }
}

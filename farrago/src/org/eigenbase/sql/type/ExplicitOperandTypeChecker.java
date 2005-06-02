/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
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
package org.eigenbase.sql.type;

import org.eigenbase.sql.*;
import org.eigenbase.sql.validate.*;
import org.eigenbase.reltype.*;
import org.eigenbase.util.*;

import java.util.*;

/**
 * Operand type-checking strategy which checks operands against an array
 * of types.
 *
 * <p>For example, the following would allow
 * <code>foo(Date)</code> or
 * <code>foo(INTEGER, INTEGER)</code> but disallow
 * <code>foo(INTEGER)</code>.
 *
 * <blockquote><pre>SimpleOperandsTypeChecking(new SqlTypeName[][] {
 *     {SqlTypeName.Date},
 *     {SqlTypeName.Int, SqlTypeName.Int}})</pre></blockquote>
 *
 * @author Wael Chatila
 * @version $Id$
 */
public class ExplicitOperandTypeChecker
    implements SqlOperandTypeChecker
{
    protected SqlTypeName[][] types;

    /**
     * Creates a SimpleOperandsTypeChecking object.
     *
     * @pre types != null
     * @pre types.length > 0
     */
    public ExplicitOperandTypeChecker(SqlTypeName[][] typeses)
    {
        Util.pre(null != typeses, "null!=types");

        //only Null types specified? Prohibit! need more than null
        for (int i = 0; i < typeses.length; i++) {
            final SqlTypeName[] types = typeses[i];
            Util.pre(types.length > 0, "Need to define a type");
            boolean foundOne = false;
            for (int j = 0; j < types.length; j++) {
                SqlTypeName type = types[j];
                if (!type.equals(SqlTypeName.Null)) {
                    foundOne = true;
                    break;
                }
            }

            if (!foundOne) {
                Util.pre(false, "Must have at least one non-null type");
            }
        }

        this.types = typeses;
    }

    public boolean check(
        SqlCall call,
        SqlValidator validator,
        SqlValidatorScope scope,
        SqlNode node,
        int ruleOrdinal,
        boolean throwOnFailure)
    {
        RelDataType actualType = null;

        // for each operand, iterate over its allowed types...
        for (int j = 0; j < types[ruleOrdinal].length; j++) {
            SqlTypeName expectedTypeName = types[ruleOrdinal][j];
            if (SqlTypeName.Any.equals(expectedTypeName)) {
                // If the argument type is defined as any type, we don't need
                // to check
                return true;
            } else {
                if( null == actualType) {
                    actualType = validator.deriveType(scope, node);
                }

                if (expectedTypeName.equals(actualType.getSqlTypeName())) {
                    return true;
                }
            }
        }

        if (throwOnFailure) {
            throw call.newValidationSignatureError(validator, scope);
        }
        return false;
    }

    public boolean check(
        SqlValidator validator,
        SqlValidatorScope scope,
        SqlCall call,
        boolean throwOnFailure)
    {
        assert (getArgCount() == call.operands.length);

        for (int i = 0; i < call.operands.length; i++) {
            SqlNode operand = call.operands[i];
            if (!check(call, validator, scope, operand, i, throwOnFailure)) {
                return false;
            }
        }
        return true;
    }

    public int getArgCount()
    {
        return types.length;
    }

    public SqlTypeName [][] getTypes()
    {
        return types;
    }

    public String getAllowedSignatures(SqlOperator op)
    {
        StringBuffer buf = new StringBuffer();
        ArrayList list = new ArrayList();
        getAllowedSignatures(0, list, buf, op);
        return buf.toString().trim();
    }

    /**
     * Helper function to {@link
     * #getAllowedSignatures(org.eigenbase.sql.SqlOperator)}
     */
    protected void getAllowedSignatures(
        int depth,
        ArrayList list,
        StringBuffer buf,
        SqlOperator op)
    {
        assert (null != types[depth]);
        assert (types[depth].length > 0);

        for (int i = 0; i < types[depth].length; i++) {
            SqlTypeName type = types[depth][i];
            if (type.equals(SqlTypeName.Null)) {
                continue;
            }

            list.add(type);
            if ((depth + 1) < types.length) {
                getAllowedSignatures(depth + 1, list, buf, op);
            } else {
                buf.append(op.getAnonymousSignature(list));
                buf.append(SqlOperator.NL);
            }
            list.remove(list.size() - 1);
        }
    }
}

// End ExplicitOperandTypeChecker.java

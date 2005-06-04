/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.resource.EigenbaseResource;
import org.eigenbase.sql.*;
import org.eigenbase.sql.validate.SqlValidatorScope;
import org.eigenbase.sql.validate.SqlValidator;
import org.eigenbase.sql.test.SqlTester;
import org.eigenbase.sql.type.*;

/**
 * SqlMultisetOperator represents the SQL:2003 standard MULTISET constructor
 *
 * @author Wael Chatila
 * @since Oct 17, 2004
 * @version $Id$
 */
public class SqlMultisetOperator extends SqlSpecialOperator
{
    //~ Constructors ----------------------------------------------------------

    public SqlMultisetOperator(SqlKind kind)
    {
        // Precedence of 100 because nothing can pull parentheses apart.
        super("MULTISET", kind, 100, false,
            SqlTypeStrategies.rtiFirstArgType,
            null,
            SqlTypeStrategies.otcVariadic);
        assert(kind.isA(SqlKind.MultisetQueryConstructor) ||
               kind.isA(SqlKind.MultisetValueConstructor));

    }

    //~ Methods ---------------------------------------------------------------

    // implement SqlOperator
    public SqlSyntax getSyntax()
    {
        return SqlSyntax.Special;
    }

    protected RelDataType getType(
        SqlValidator validator,
        SqlValidatorScope scope,
        RelDataTypeFactory typeFactory,
        CallOperands callOperands)
    {
        RelDataType type = getComponentType(typeFactory,  callOperands.collectTypes());
        if (null == type) {
            return null;
        }
        return SqlTypeUtil.createMultisetType(typeFactory, type, false);
    }

    private RelDataType getComponentType(RelDataTypeFactory typeFactory,
        RelDataType[] argTypes)
    {
        return SqlTypeUtil.getNullableBiggest(typeFactory, argTypes);
    }

    protected boolean checkArgTypes(
        SqlCall call,
        SqlValidator validator,
        SqlValidatorScope scope,
        boolean throwOnFailure)
    {
        final RelDataType[] argTypes =
            SqlTypeUtil.collectTypes(validator, scope, call.operands);
        final RelDataType componentType = getComponentType(
            validator.getTypeFactory(), argTypes);
        if (null == componentType) {
            if (throwOnFailure) {
                throw validator.newValidationError(call,
                    EigenbaseResource.instance().newNeedSameTypeParameter());
            }
            return false;
        }
        return true;
    }

    public void test(SqlTester tester)
    {
//        SqlOperatorTests.testMultiset();
    }

    public void unparse(
        SqlWriter writer,
        SqlNode[] operands,
        int leftPrec,
        int rightPrec) {

        writer.print("MULTISET");
        if (getKind().isA(SqlKind.MultisetValueConstructor)) {
            writer.print("[");
        } else {
            writer.print("(");
        }
        for (int i = 0; i < operands.length; i++) {
            if (i>0) {
                assert(getKind().isA(SqlKind.MultisetValueConstructor));
                writer.print(", ");
            }
            operands[i].unparse(writer, leftPrec, rightPrec);
        }
        if (getKind().isA(SqlKind.MultisetValueConstructor)) {
            writer.print("]");
        } else {
            writer.print(")");
        }
    }
}


// End SqlRowOperator.java


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
import org.eigenbase.resource.EigenbaseResource;
import org.eigenbase.sql.*;
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

    public SqlMultisetOperator()
    {
        // Precedence of 100 because nothing can pull parentheses apart.
        super("MULTISET", SqlKind.Multiset, 100, false,
            ReturnTypeInferenceImpl.useFirstArgType,
            UnknownParamInference.useFirstKnown, null);
    }

    //~ Methods ---------------------------------------------------------------

    // implement SqlOperator
    public SqlSyntax getSyntax()
    {
        return SqlSyntax.Special;
    }

    // implement SqlOperator
    public SqlOperator.OperandsCountDescriptor getOperandsCountDescriptor()
    {
        return OperandsCountDescriptor.variadicCountDescriptor;
    }

    protected RelDataType getType(
        SqlValidator validator,
        SqlValidator.Scope scope,
        RelDataTypeFactory typeFactory,
        CallOperands callOperands)
    {
        RelDataType type = getComponentType(typeFactory,  callOperands.collectTypes());
        if (null == type) {
            return null;
        }
        RelDataType ret = typeFactory.createMultisetType(type);
        ret = typeFactory.createTypeWithNullability(ret, type.isNullable());
        return ret;
    }

    private RelDataType getComponentType(RelDataTypeFactory typeFactory,
        RelDataType[] argTypes)
    {
        return SqlTypeUtil.getNullableBiggest(typeFactory, argTypes);
    }

    protected boolean checkArgTypes(
        SqlCall call,
        SqlValidator validator,
        SqlValidator.Scope scope,
        boolean throwOnFailure)
    {
        if (null==getComponentType(validator.typeFactory,
                SqlTypeUtil.collectTypes(validator, scope, call.operands))) {
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

        writer.print("MULTISET[");
        for (int i = 0; i < operands.length; i++) {
            if (i>0) {
                writer.print(", ");
            }
            operands[i].unparse(writer, leftPrec, rightPrec);
        }
        writer.print("]");
    }
}


// End SqlRowOperator.java


/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2004 Disruptive Tech
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
package org.eigenbase.sql.fun;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.resource.EigenbaseResource;
import org.eigenbase.sql.*;
import org.eigenbase.sql.test.SqlTester;
import org.eigenbase.util.Util;

import java.util.ArrayList;
import java.util.List;

/**
 * SqlCastFunction.  Note that the std functions are really singleton objects,
 * because they always get fetched via the StdOperatorTable.  So you can't
 * story any local info in the class and hence the return type data is maintained
 * in operand[1] through the validation phase.
 *
 * @author lee
 * @since Jun 5, 2004
 * @version $Id$
 **/
public class SqlCastFunction extends SqlFunction {

    public SqlCastFunction() {
        super("CAST",SqlKind.Cast, null, SqlOperatorTable.useFirstKnownParam,
                null, SqlFunction.SqlFuncTypeName.System);
    }

    // private SqlNode returnNode;
    /**
     * Creates a call to this operand with an array of operands.
     */
    /* public SqlCall createCall(SqlNode [] operands) {
    assert(operands.length == 2); // the parser should choke on more than 2 operands? test
    returnNode = operands[1];
    return super.createCall(new SqlNode [] {operands[0]});
    } */

    public RelDataType getType(RelDataTypeFactory factory, RelDataType [] argTypes) {
        assert argTypes.length == 2;
        return argTypes[1];
    }

    protected String getSignatureTemplate(final int operandsCount) {
        switch (operandsCount) {
        case 2: return "{0}({1} AS {2})";
        }
        assert(false);
        return null;
    }

    public OperandsCountDescriptor getOperandsCountDescriptor() {
        return new OperandsCountDescriptor(2);
    }

    protected void checkNumberOfArg(SqlCall call) {
        if (2 != call.operands.length) {
            throw Util.newInternal("todo: Wrong number of arguments to " + call);
        }
    }


    /**
     * Makes sure that the number and types of arguments are allowable.
     * Operators (such as "ROW" and "AS") which do not check their arguments
     * can override this method.
     */
    protected void checkArgTypes(SqlCall call, SqlValidator validator, SqlValidator.Scope scope) {
        if (SqlUtil.isNullLiteral(call.operands[0], false)) {
            return;
        }
        RelDataType validatedNodeType = validator.getValidatedNodeType(call.operands[0]);
        RelDataType returnType = ((SqlDataType)call.operands[1]).getType();
        if (!returnType.isAssignableFrom(validatedNodeType, true)) {
            throw  EigenbaseResource.instance().newCannotCastValue(validatedNodeType.toString(), returnType.toString());
        }
    }

    /**
     * Figure out the type of the return of this function.
     * We have already checked that the number and types of arguments are as
     * required.
     */
    protected RelDataType inferType(SqlValidator validator,
            SqlValidator.Scope scope, SqlCall call) {
        RelDataType ret = ((SqlDataType) call.getOperands()[1]).getType();
        boolean isNullable;
        if (SqlUtil.isNullLiteral(call.operands[0], false)) {
            isNullable = true;
        } else {
            RelDataType firstType = validator.getValidatedNodeType(call.operands[0]);
            isNullable = firstType.isNullable();
        }
        ret = validator.typeFactory.createTypeWithNullability(ret, isNullable);
        validator.setValidatedNodeType(call.operands[0], ret);
        return ret;
    }

    public void unparse(
            SqlWriter writer,
            SqlNode[] operands,
            int leftPrec,
            int rightPrec) {
        writer.print(name);
        writer.print('(');
        for (int i = 0; i < operands.length; i++) {
            SqlNode operand = operands[i];
            if (i > 0) {
                writer.print(" AS ");
            }
            operand.unparse(writer,0,0);
        }
        writer.print(')');
    }

    /**
     * An abstract method where its implementations call the
     * {@link SqlTester}'s
     * different <code>checkXXX</code> methods.
     * An example test function for the sin operator
     * <blockqoute><pre><code>
     * void test(SqlTester tester) {<br>
     *     tester.checkScalar("sin(0)", "0");<br>
     *     tester.checkScalar("sin(1.5707)", "1");<br>
     * }<br>
     * </code></pre></blockqoute>
     * @param tester The tester to use.
     */

    public void test(SqlTester tester) {
        tester.checkScalarExact("cast(1.0 as integer)","1");
        tester.checkScalarApprox("cast(1 as double)","1.0");
        tester.checkScalarApprox("cast(1.0 as double)","1.0");
        tester.checkNull("cast(null as double)");
        tester.checkNull("cast(null as date)");
    }

}

// End SqlCastFunction.java

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

package org.eigenbase.sql;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.sql.parser.SqlParserPos;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.validate.SqlValidator;
import org.eigenbase.sql.validate.SqlValidatorScope;
import org.eigenbase.resource.EigenbaseResource;


/**
 * A <code>SqlFunction</code> is a type of operator which has conventional
 * function-call syntax.
 */
public class SqlFunction extends SqlOperator
{
    //~ Instance fields -------------------------------------------------------

    private final SqlFunctionCategory functionType;

    private final SqlIdentifier sqlIdentifier;

    private final RelDataType [] paramTypes;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new SqlFunction for a call to a builtin function.
     *
     * @param name of builtin function
     *
     * @param kind kind of operator implemented by function
     *
     * @param returnTypeInference strategy to use for return type inference
     *
     * @param operandTypeInference strategy to use for parameter type inference
     *
     * @param operandTypeChecker strategy to use for parameter type checking
     *
     * @param funcType categorization for function
     */
    public SqlFunction(
        String name,
        SqlKind kind,
        SqlReturnTypeInference returnTypeInference,
        SqlOperandTypeInference operandTypeInference,
        SqlOperandTypeChecker operandTypeChecker,
        SqlFunctionCategory funcType)
    {
        super(name, kind, 100, 100, returnTypeInference, operandTypeInference,
            operandTypeChecker);
        
        assert !(funcType == SqlFunctionCategory.UserDefinedConstructor &&
            returnTypeInference == null);

        this.functionType = funcType;

        // NOTE jvs 18-Jan-2005:  we leave sqlIdentifier as null to indicate
        // that this is a builtin.  Same for paramTypes.
        this.sqlIdentifier = null;
        this.paramTypes = null;
    }

    /**
     * Creates a placeholder SqlFunction for an invocation of a function with a
     * possibly qualified name.  This name must be resolved into either a
     * builtin function or a user-defined function.
     *
     * @param sqlIdentifier possibly qualified identifier for function
     *
     * @param returnTypeInference strategy to use for return type inference
     *
     * @param operandTypeInference strategy to use for parameter type inference
     *
     * @param operandTypeChecker strategy to use for parameter type checking
     *
     * @param paramTypes array of parameter types
     *
     * @param funcType function category
     */
    public SqlFunction(
        SqlIdentifier sqlIdentifier,
        SqlReturnTypeInference returnTypeInference,
        SqlOperandTypeInference operandTypeInference,
        SqlOperandTypeChecker operandTypeChecker,
        RelDataType [] paramTypes,
        SqlFunctionCategory funcType)
    {
        super(
            sqlIdentifier.names[sqlIdentifier.names.length - 1],
            SqlKind.Function,
            100,
            100,
            returnTypeInference,
            operandTypeInference,
            operandTypeChecker);

 //       assert !(funcType == SqlFunctionCategory.UserDefinedConstructor &&
 //           returnTypeInference == null);

        this.sqlIdentifier = sqlIdentifier;
        this.functionType = funcType;
        this.paramTypes = paramTypes;
    }

    //~ Methods ---------------------------------------------------------------

    public SqlSyntax getSyntax()
    {
        return SqlSyntax.Function;
    }

    /**
     * @return fully qualified name of function, or null for a builtin
     * function
     */
    public SqlIdentifier getSqlIdentifier()
    {
        return sqlIdentifier;
    }

    /**
     * @return fully qualified name of function
     */
    public SqlIdentifier getNameAsId()
    {
        if (sqlIdentifier != null) {
            return sqlIdentifier;
        }
        return new SqlIdentifier(getName(), SqlParserPos.ZERO);
    }

    /**
     * @return array of parameter types, or null for builtin function
     */
    public RelDataType [] getParamTypes()
    {
        return paramTypes;
    }

    public void unparse(
        SqlWriter writer,
        SqlNode [] operands,
        int leftPrec,
        int rightPrec)
    {
        getSyntax().unparse(writer, this, operands, leftPrec, rightPrec);
    }

    /**
     * @return function category
     */
    public SqlFunctionCategory getFunctionType()
    {
        return this.functionType;
    }

    public boolean isQuantifierAllowed()
    {
        return(false);
    }

    /*
     * Validates a call to this operator.
     * <p/>
     *
     * This implementation looks for the quantifier keywords DISTINCT or ALL
     * as te first operand in the list.  If found then the literal is not
     * called to validate itself.  Further the function is checked to make
     * sure that a quantifier is valid for that particular function.
     *
     * If the first operand does not appear to be a quantifier then the
     * parent ValidateCall is invoked to do normal function validation.
     *
     * @param call the call to this operator
     * @param validator the active validator
     * @param scope validator scope
     * @param operandScope validator scope in which to validate operands to
     * this call; usually equal to scope, but not always because some operators
     * introduce new scopes
     */
    public void validateCall(
        SqlCall call,
        SqlValidator validator,
        SqlValidatorScope scope,
        SqlValidatorScope operandScope)
    {
        super.validateCall(call, validator, scope, operandScope);
        if ((null != call.getFunctionQuantifier()) && !isQuantifierAllowed()) {
            throw validator.newValidationError(call.getFunctionQuantifier(),
                EigenbaseResource.instance()
                .FunctionQuantifierNotAllowed.ex(call.getOperator().getName()));
        }
    }

}


// End SqlFunction.java

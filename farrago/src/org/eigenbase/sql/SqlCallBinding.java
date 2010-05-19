/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
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

import java.util.*;

import org.eigenbase.reltype.*;
import org.eigenbase.resource.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.sql.validate.*;
import org.eigenbase.util.*;


/**
 * <code>SqlCallBinding</code> implements {@link SqlOperatorBinding} by
 * analyzing to the operands of a {@link SqlCall} with a {@link SqlValidator}.
 *
 * @author Wael Chatila
 * @version $Id$
 */
public class SqlCallBinding
    extends SqlOperatorBinding
{
    //~ Instance fields --------------------------------------------------------

    private final SqlValidator validator;
    private final SqlValidatorScope scope;
    private final SqlCall call;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a call binding.
     *
     * @param validator Validator
     * @param scope Scope of call
     * @param call Call node
     */
    public SqlCallBinding(
        SqlValidator validator,
        SqlValidatorScope scope,
        SqlCall call)
    {
        super(
            validator.getTypeFactory(),
            call.getOperator());
        this.validator = validator;
        this.scope = scope;
        this.call = call;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Returns the validator.
     */
    public SqlValidator getValidator()
    {
        return validator;
    }

    /**
     * Returns the scope of the call.
     */
    public SqlValidatorScope getScope()
    {
        return scope;
    }

    /**
     * Returns the call node.
     */
    public SqlCall getCall()
    {
        return call;
    }

    // implement SqlOperatorBinding
    public String getStringLiteralOperand(int ordinal)
    {
        SqlNode node = call.operands[ordinal];
        return SqlLiteral.stringValue(node);
    }

    // implement SqlOperatorBinding
    public int getIntLiteralOperand(int ordinal)
    {
        //todo: move this to SqlTypeUtil
        SqlNode node = call.operands[ordinal];
        if (node instanceof SqlLiteral) {
            SqlLiteral sqlLiteral = (SqlLiteral) node;
            return sqlLiteral.intValue(true);
        } else if (node instanceof SqlCall) {
            final SqlCall c = (SqlCall) node;
            if (c.isA(SqlKind.MinusPrefix)) {
                SqlNode child = c.operands[0];
                if (child instanceof SqlLiteral) {
                    return -((SqlLiteral) child).intValue(true);
                }
            }
        }
        throw Util.newInternal("should never come here");
    }

    // implement SqlOperatorBinding
    public boolean isOperandNull(int ordinal, boolean allowCast)
    {
        return SqlUtil.isNullLiteral(call.operands[ordinal], allowCast);
    }

    // implement SqlOperatorBinding
    public int getOperandCount()
    {
        return call.operands.length;
    }

    // implement SqlOperatorBinding
    public RelDataType getOperandType(int ordinal)
    {
        final SqlNode operand = call.operands[ordinal];
        final RelDataType type = validator.deriveType(scope, operand);
        final SqlValidatorNamespace namespace = validator.getNamespace(operand);
        if (namespace != null) {
            return namespace.getRowTypeSansSystemColumns();
        }
        return type;
    }

    public RelDataType getCursorOperand(int ordinal)
    {
        final SqlNode operand = call.operands[ordinal];
        if (!SqlUtil.isCallTo(operand, SqlStdOperatorTable.cursorConstructor)) {
            return null;
        }
        final SqlCall cursorCall = (SqlCall) operand;
        final SqlNode query = cursorCall.operands[0];
        return validator.deriveType(scope, query);
    }

    // implement SqlOperatorBinding
    public String getColumnListParamInfo(
        int ordinal,
        String paramName,
        List<String> columnList)
    {
        final SqlNode operand = call.operands[ordinal];
        if (!SqlUtil.isCallTo(operand, SqlStdOperatorTable.rowConstructor)) {
            return null;
        }
        SqlNode [] operands = ((SqlCall) operand).getOperands();
        for (int i = 0; i < operands.length; i++) {
            SqlIdentifier id = (SqlIdentifier) operands[i];
            columnList.add(id.getSimple());
        }
        return validator.getParentCursor(paramName);
    }

    public EigenbaseException newError(
        SqlValidatorException e)
    {
        return validator.newValidationError(call, e);
    }

    /**
     * Constructs a new validation signature error for the call.
     *
     * @return signature exception
     */
    public EigenbaseException newValidationSignatureError()
    {
        return validator.newValidationError(
            call,
            EigenbaseResource.instance().CanNotApplyOp2Type.ex(
                getOperator().getName(),
                call.getCallSignature(validator, scope),
                getOperator().getAllowedSignatures()));
    }

    /**
     * Constructs a new validation error for the call. (Do not use this to
     * construct a validation error for other nodes such as an operands.)
     *
     * @param ex underlying exception
     *
     * @return wrapped exception
     */
    public EigenbaseException newValidationError(
        SqlValidatorException ex)
    {
        return validator.newValidationError(call, ex);
    }
}

// End SqlCallBinding.java

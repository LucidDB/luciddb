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

package org.eigenbase.sql;

import java.util.ArrayList;
import java.util.HashMap;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.resource.EigenbaseResource;
import org.eigenbase.sql.parser.ParserPosition;
import org.eigenbase.sql.util.SqlVisitor;
import org.eigenbase.util.Util;

/**
 * A <code>SqlCall</code> is a call to an {@link SqlOperator operator}.
 * (Operators can be used to describe any syntactic construct, so in
 * practice, every non-leaf node in a SQL parse tree is a
 * <code>SqlCall</code> of some kind.)
 */
public class SqlCall extends SqlNode
{
    //~ Instance fields -------------------------------------------------------

    public SqlOperator operator;
    public final SqlNode [] operands;

    //~ Constructors ----------------------------------------------------------

    SqlCall(
        SqlOperator operator,
        SqlNode [] operands,
        ParserPosition pos)
    {
        super(pos);
        this.operator = operator;
        this.operands = operands;
    }

    //~ Methods ---------------------------------------------------------------

    public boolean isA(SqlKind kind)
    {
        return operator.kind.isA(kind);
    }

    public SqlKind getKind()
    {
        return operator.kind;
    }

    // REVIEW jvs 10-Sept-2003:  I added this to allow for some rewrite by
    // SqlValidator.  Is mutability OK?
    public void setOperand(
        int i,
        SqlNode operand)
    {
        operands[i] = operand;
    }

    public SqlNode [] getOperands()
    {
        return operands;
    }

    public Object clone()
    {
        return operator.createCall(
            SqlNode.cloneArray(operands),
            getParserPosition());
    }

    public void unparse(
        SqlWriter writer,
        int leftPrec,
        int rightPrec)
    {
        if ((leftPrec > operator.leftPrec)
                || (operator.rightPrec <= rightPrec)
                || (writer.alwaysUseParentheses && isA(SqlKind.Expression))) {
            writer.print('(');
            operator.unparse(writer, operands, 0, 0);
            writer.print(')');
        } else {
            operator.unparse(writer, operands, leftPrec, rightPrec);
        }
    }

    /**
     * Validates this call.
     *
     * <p>The default implementation delegates the validation to the operator's
     * {@link SqlOperator#validateCall}. Derived classes may override (as do,
     * for example {@link SqlSelect} and {@link SqlUpdate}).
     */
    public void validate(SqlValidator validator, SqlValidator.Scope scope)
    {
        validator.validateCall(this, scope);
    }

    /**
     * Find out all the valid alternatives for the operand of this node's 
     * operator that matches the parse position indicated by pp
     *
     * @param validator Validator
     * @param scope Validation scope
     * @param pp ParserPosition indicating the cursor position at which 
     * competion hints are requested for
     * @return a string array of valid options
     */
    public String[] findValidOptions(SqlValidator validator, 
        SqlValidator.Scope scope,
        ParserPosition pp)
    {
        final SqlNode[] operands = getOperands();
        HashMap sqlids = new HashMap();
        for (int i = 0; i < operands.length; i++) {
            if (operands[i] instanceof SqlIdentifier) {
                SqlIdentifier id = (SqlIdentifier) operands[i];
                String ppstring = id.getParserPosition().toString();
                if (ppstring.equals(pp.toString())) {
                    return id.findValidOptions(validator, scope);
                }
            }
        }
        return Util.emptyStringArray;
    }

    public void accept(SqlVisitor visitor)
    {
        visitor.visit(this);
    }

    public boolean equalsDeep(SqlNode node)
    {
        if (!(node instanceof SqlCall)) {
            return false;
        }
        SqlCall that = (SqlCall) node;
        // Compare operators by name, not identity, because they may not
        // have been resolved yet.
        if (!this.operator.name.equals(that.operator.name)) {
            return false;
        }
        if (this.operands.length != that.operands.length) {
            return false;
        }
        for (int i = 0; i < this.operands.length; i++) {
            if (!this.operands[i].equalsDeep(that.operands[i])) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns a string describing the actual argument types of a call, e.g.
     * "SUBSTR(VARCHAR(12), NUMBER(3,2), INTEGER)".
     */
    protected String getCallSignature(
        SqlValidator validator,
        SqlValidator.Scope scope)
    {

        ArrayList signatureList = new ArrayList();
        for (int i = 0; i < operands.length; i++) {
            final SqlNode operand = operands[i];
            final RelDataType argType = validator.deriveType(scope, operand);
            if (null == argType) {
                continue;
            }
            signatureList.add(argType.toString());
        }
        return operator.getSignature(signatureList);

    }

    public RuntimeException newValidationSignatureError(
        SqlValidator validator,
        SqlValidator.Scope scope)
    {
        return validator.newValidationError(
            this,
            EigenbaseResource.instance().newCanNotApplyOp2Type(
                operator.name,
                getCallSignature(validator, scope),
                operator.getAllowedSignatures()));
    }
}


// End SqlCall.java
